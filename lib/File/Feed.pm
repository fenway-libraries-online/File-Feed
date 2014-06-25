package File::Feed;

use strict;
use warnings;

use File::Kvpar;
use File::Kit;
use File::Feed::Channel;
use File::Feed::Source;
use File::Path qw(mkpath);
use File::Copy qw(move copy);
use File::Basename qw(basename dirname);

use vars qw($VERSION);

$VERSION = '0.02';

# Feed statuses
use constant IDLE     => '@idle';
use constant FILLING  => '@filling';
use constant DRAINING => '@draining';
use constant ERROR    => '@error';
use constant FROZEN   => '@frozen';

sub new {
    my ($cls, $dir) = @_;
    my $kv = File::Kvpar->new('+<', "$dir/feed.kv");
    my ($head, @channels) = $kv->elements;
    if ($head->{'perl-class'}) {
        $cls = $head->{'perl-class'};
        eval "use $cls; 1" or die "Can't use feed class $cls: $!";
    }
    @channels = grep { $_->{'@'} eq 'channel' } @channels;
    @channels = ({
        'from' => '.',
        'to' => '.',
        'description' => 'default channel',
        'filter' => 'glob:*',
    }) if !@channels;
    my $self = bless {
        '_dir' => $dir,
        '_fileskv' => File::Kvpar->new('+<', "$dir/files.kv"),
        %$head,
        '_random_buf' => '',
    }, $cls;
    $self->{'_source'} = $self->_source_instance('uri' => $head->{'source'});
    $self->{'_feedkv'} = $kv;
    $self->{'_channels'} = [ map { $self->_channel_instance(%$_) } @channels ];
    return $self;
}

sub fill {
    my $self = shift;
    my @files;
    my @chan = $self->channels(@_);
    my $source = $self->source;
    my $ok = eval {
        $self->status(FILLING) or die "Can't set status: not a feed?";
        my %logged = map { $_->{'to'} => $_ } $self->files;
        $source->begin($self);
        foreach my $chan (@chan) {
            my ($from_dir, $to_dir, $filter, $recursive, $autodir)
                = ($chan->from, $chan->to, $chan->file_filter, $chan->recursive, $chan->autodir);
            my ($repeat, $clobber)
                = ($chan->repeat || $self->repeat, $chan->clobber || $self->clobber);
            foreach ($source->list($from_dir, $recursive)) {
                (my $path = $_) =~ s{^$from_dir/}{};
                my ($from, $to) = ($_, "$to_dir/$path");
                next if !$filter->($path);
                my $replace;
                next if $logged{$to} && ! $repeat;
                my $arch = $self->path('archive', $to);
                my $dest = $self->path('new',     $to);
                my $dest_dir = dirname($dest);
                my $arch_dir = dirname($arch);
                if ($clobber) {
                    $self->_shadow($arch) if -e $arch;
                    unlink $arch;
                }
                elsif (-e $arch) {
                    die "File $to would clobber $arch";
                }
                else {
                    mkpath $arch_dir if ! -d $arch_dir;
                }
                if (! -d $dest_dir) {
                    die "Destination directory $dest_dir does not exist"
                        if !$autodir;
                    mkpath($dest_dir);
                }
                if ($source->fetch($from, $dest)) {
                    link $dest, $arch or die;
                    push @files, $self->_file_instance(
                        '#'       => $from,
                        'feed'    => $self->id,
                        'source'  => $source->uri,
                        'channel' => $chan->id,
                        'from'    => $from,
                        'to'      => $to,
                    );
                }
            }
        }
        $source->end;
        $self->{'_fileskv'}->append(@files);
        1;
    };
    $self->_cleanup;
    $self->status($ok ? IDLE : ERROR);
    die "Fill failed: $@\n" if !$ok;
    return @files;
}

sub _source_instance {
    my $self = shift;
    return File::Feed::Source->new(@_);
}

sub _channel_instance {
    my $self = shift;
    return File::Feed::Channel->new('_feed' => $self, @_)
}

sub _file_instance {
    my $self = shift;
    return File::Feed::File->new(@_);
}

sub _kit_instance {
    my $self = shift;
    return File::Kit->new(@_);
}

sub new_files {
    my $self = shift;
    # What channels should we look at?
    my %chan = map { $_ => 1 } $self->channels(@_);
    # Within those channels, what files are new?
    my $new_dir = $self->path('new');
    my @new;
    _crawl($new_dir, \@new);
    s{^$new_dir/}{} for @new;
    my %new  = map { $_ => 1 } @new;
    # Instantiate all files meeting those criteria
    return map {
               $self->_file_instance(%$_)
           }
           grep {
               $new{$_->{'to'}}
               &&
               $chan{$_->{'channel'}}
           }
           $self->files;
}

sub _crawl {
    my ($dir, $list) = @_;
    opendir my $fh, $dir or die;
    my @files = grep { !/^\./ } readdir($fh);
    closedir $fh;
    foreach my $name (@files) {
        my $path = "$dir/$name";
        if (-d $path) {
            _crawl($path, $list);
        }
        elsif (-f _) {
            push @$list, $path;
        }
    }
}

sub drain {
    my $self = shift;
    unshift @_, 'to' if @_ % 2;
    my %arg = @_;
    my $to = delete $arg{'to'}
        or die "No destination for drain";
    my $strip = $arg{'strip_leading_components'};
    my %want = map { $_ => 1 } $self->channels( @{ $arg{'channels'} || [] } );
    my @new = $self->new_files(%arg);
    return if !@new;
    my $autodir = $self->autodir;
    my $ok = eval {
        $self->status(DRAINING) or die "Can't set status: not a feed?";
        my $r = ref $to;
        if ($r eq '') {
            if (! -d $to) {
                die "Destination directory $to does not exist"
                    if !$arg{'autodir'};
                mkpath($to);
            }
            my %have_dir;
            foreach my $file (@new) {
                next if !$want{$file->channel};
                my $path = my $dest_path = $file->to;
                if ($strip) {
                    my $n = $strip;
                    $dest_path =~ s{^[^/]+/}{} while $n--;
                }
                my ($new, $dest) = ($self->path('new', $path), "$to/$dest_path");
                my $dest_dir = dirname($dest);
                mkpath($dest_dir) if ! $have_dir{$dest_dir}++ && !-d $dest_dir;
                if (-e $dest) {
                    if ($arg{'clobber'}) {
                        print STDERR "File $dest already exists, deleting it\n";
                        unlink $dest;
                    }
                    else {
                        print STDERR "File $dest already exists, skipping $new\n";
                        next;
                    }
                }
                $self->_shadow($new);
                move($new, $dest) or die "Can't move $path to $dest_dir: $!";
            }
        }
        elsif ($r eq 'CODE') {
            foreach my $file (@new) {
                $self->_shadow($self->path('new', $file->path));
                $to->($file);
            }
        }
        1;
    };
    $self->_cleanup;
    $self->status($ok ? IDLE : ERROR);
    die "Fill failed: $@\n" if !$ok;
}

sub path {
    my $self = shift;
    return join '/', $self->dir, @_;
}

sub assemble {
    my $self = shift;
    my $dir = $self->dir;
    my $asm = $self->path('bin', 'assemble');
    if (-x $asm) {
        system($asm, @_);
    }
    else {
        foreach (@_) {
            my $file = $self->_file_instance(%$_);
            my $kit = $self->_kit_instance($file);
        }
    }
}

sub status {
    my ($self, $new_status, $error_ok) = @_;
    my $dir = $self->dir;
    my ($old_status, @etc) = map { basename($_) } glob($self->path('@*'));
    die "No status set for feed $self->{'@'}" if !defined $old_status;
    die "Multiple statuses set for feed $self->{'@'}" if @etc;
    return $old_status if !defined $new_status;
    die "Feed is frozen" if $old_status eq FROZEN;
    die "Feed is in error state" if $old_status eq ERROR && !$error_ok;
    return $new_status if $old_status eq $new_status;
    rename $self->path($old_status), $self->path($new_status)
        or die "Can't set status for feed $self->{'@'}: $!";
    return $old_status;
}

sub id          { $_[0]->{'#'}           }
sub host        { $_[0]->source->host    }
sub root        { $_[0]->source->root    }
sub from        { $_[0]->{'from'} || $_[0]->id }
sub to          { $_[0]->{'to'} || $_[0]->id }
sub description { $_[0]->{'description'} }
sub user        { $_[0]->{'user'}        }
sub autodir     { $_[0]->{'autodir'}     }
sub repeat      { $_[0]->{'repeat'}      }
sub clobber     { $_[0]->{'clobber'}   }

sub dir         { $_[0]->{'_dir'}        }
sub source      { $_[0]->{'_source'}     }

sub channels {
    my $self = shift;
    my @chan = @{ $self->{'_channels'} };
    return @chan if !@_;
    my %want;
    foreach my $spec (@_) {
        if (s/^(pcre|regexp)://) {
            my $rx = qr/$spec/;
            $want{$_} = 1 for grep { $_ =~ $rx } @chan;
        }
        elsif (s/^glob:// || m{(?:^|/)[*](?:/|$)}) {
            my $rx = _pattern2regexp($spec);
            $want{$_} = 1 for grep { $_ =~ $rx } @chan;
        }
        else {
            $want{$spec} = 1;
        }
    }
    return grep { $want{$_->id} } @chan;
}

sub files {
    my ($self) = @_;
    return $self->{'_fileskv'}->elements;
}

sub _shadow {
    my ($self, $file) = @_;
    my $n = 4;
    my ($rand, $shadow);
    my $dir = $self->path('shadow');
    -d $dir or mkdir $dir or die "Can't mkdir $dir $!";
    while (defined($rand = $self->_random_hex(8)) && !link $file, $shadow = $self->path('shadow', "shadow.$rand")) {
        die "Can't create shadow file $shadow for $file: $!"
            if --$n == 0;
    }
    push @{ $self->{'_shadow'} ||= [] }, [ $shadow, $file ];
    return $shadow, $file;
}

sub _cleanup {
    my ($self) = @_;
    my $shadow = $self->{'_shadow'};
    foreach (@$shadow) {
        my ($shadow, $original) = @$_;
        unlink $shadow or link($shadow, $original) or die "Can't remove shadow file $shadow or relink it to $original: $!";
    }
}

sub _fill_random_buffer {
    my ($self) = @_;
    my $fh;
    open $fh, '<', '/dev/urandom' or
    open $fh, '<', '/dev/random'  or die "Can't open /dev/*random: $!";
    sysread $fh, $self->{'_random_buf'}, 32 or die "Can't read random bytes: $!";
}

sub _random_hex {
    my ($self, $n) = @_;
    $n ||= 8;
    $n = 16 if $n > 16;
    $self->_fill_random_buffer if length($self->{'_random_buf'}) < $n;
    return lc unpack('H*', substr($self->{'_random_buf'}, 0, $n, ''));
}

1;

=pod

=head1 NAME

File::Feed - gather files from an FTP server or other source

=cut
