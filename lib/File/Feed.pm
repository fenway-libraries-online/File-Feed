package File::Feed;

use strict;
use warnings;

use File::Kvpar;
use File::Kit;
use File::Feed::Channel;
use File::Feed::Source;
use File::Path qw(mkpath);
use File::Basename qw(basename dirname);

use vars qw($VERSION);

$VERSION = '0.02';

# Feed statuses
use constant EMPTY    => '@empty';
use constant FILLING  => '@filling';
use constant DRAINING => '@draining';
use constant FULL     => '@full';
use constant ERROR    => '@error';
use constant FROZEN   => '@frozen';

sub new {
    my ($cls, $dir) = @_;
    my $kv = File::Kvpar->new('+<', "$dir/feed.kv");
    my ($head, @channels) = $kv->elements;
    @channels = grep { $_->{'@'} eq 'channel' } @channels;
    @channels = ({
        'from' => '.',
        'to' => '.',
        'description' => 'default channel',
        'filter' => 'glob:*',
    }) if !@channels;
    my $source = File::Feed::Source->new($head->{'source'});
    my $self = bless {
        '_dir' => $dir,
        '_source' => $source,
        '_fileskv' => File::Kvpar->new('+<', "$dir/files.kv"),
        %$head,
        '_random_buf' => '',
    }, $cls;
    $self->{'_feedkv'} = $kv;
    $self->{'_channels'} = [ map {
        File::Feed::Channel->new('_feed' => $self, %$_)
    } @channels ];
    return $self;
}

sub fill {
    my $self = shift;
    my (@chan, @files);
    if (@_) {
        my %want = map { $_ => 1 } @_;
        @chan = grep { $want{$_->id} } $self->channels;
    }
    else {
        @chan = $self->channels;
    }
    my $source = $self->source;
    my ($old_status, @shadow);
    my $ok = eval {
        $old_status = $self->status(FILLING);
        if (defined $old_status) {
            die "Feed is in error state" if $old_status eq ERROR;
            my $dir  = $self->dir;
            my %logged = map { $_->{'to'} => $_ } $self->files;
            $source->begin($self);
            foreach my $chan (@chan) {
                my ($from_dir, $to_dir, $filter, $recursive, $autodir, $repeat, $clobber) = ($chan->from, $chan->to, $chan->filter, $chan->recursive, $chan->autodir, $chan->repeat || $self->repeat, $chan->clobber || $self->clobber);
                foreach ($source->list($from_dir, $recursive)) {
                    (my $path = $_) =~ s{^$from_dir/}{};
                    my ($from, $to) = ($_, "$to_dir/$path");
                    next if !$filter->($path);
                    my $replace;
                    next if $logged{$to} && ! $repeat;
                    my $arch = "$dir/archive/$to";
                    my $dest = "$dir/new/$to";
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
                        push @files, File::Feed::File->new(
                            '#'       => $from,
                            'feed'    => $self->id,
                            'source'  => $source->uri,
                            'channel' => $chan->id,
                            'from'    => $from,
                            'to'      => $to,
                        )
                    }
                }
            }
            $source->end;
            $self->{'_fileskv'}->append(@files);
        }
        1;
    };
    if ($ok) {
        $self->status($old_status eq FULL ? FULL : @files ? FULL : EMPTY);
    }
    else {
        $self->status(ERROR) if !defined($old_status) || $old_status ne ERROR;
    }
    $self->_cleanup;
    die "Fill failed: $@\n" if !$ok;
    return @files;
}

sub full {
}

sub drain {
    my ($self, $dest_root) = @_;
    my $old_status;
    my @new = $self->full;
    return if !@new;
    my @shadow;
    my $autodir = $self->autodir;
    my $ok = eval {
        $old_status = $self->status(DRAINING);
        if (defined $old_status) {
            die "Feed is in error state" if $old_status eq ERROR;
            my $dir  = $self->dir;
            if (! -d $dest_root) {
                die "Destination directory $dest_root does not exist"
                    if !$autodir;
                mkpath($dest_root);
            }
            my %dir;
            foreach my $file (@new) {
                my $path = $file->path;
                my ($new, $dest) = ("$dir/new/$path", "$dest_root/$path");
                my $dest_dir = dirname($dest);
                mkpath($dest_dir) if ! $dir{$dest_dir}++ && ! -d $dest_dir;
                $self->_shadow($new);
                move($new, $dest) or die "Can't copy $path to $dest_dir: $!";
            }
        }
        1;
    };
    if ($ok) {
        $self->status(EMPTY);
    }
    else {
        $self->status(ERROR) if !defined($old_status) || $old_status ne ERROR;
    }
    $self->_cleanup;
    die "Fill failed: $@\n" if !$ok;
}

sub assemble {
    my $self = shift;
    my $dir = $self->dir;
    if (-x "$dir/bin/assemble") {
        system("$dir/bin/assemble", @_);
    }
    else {
        foreach (@_) {
            my $file = File::Feed::File->new(%$_);
            my $kit = File::Kit->new($file);
        }
    }
}

sub status {
    my ($self, $status) = @_;
    my $dir = $self->dir;
    my @cur_status = map { basename($_) } glob("$dir/\@*");
    die "No status set for feed $self->{'@'}" if !@cur_status;
    die "Multiple statuses set for feed $self->{'@'}" if @cur_status > 1;
    return $cur_status[0] if !defined $status;
    die "Feed is frozen" if $cur_status[0] eq FROZEN;
    return $status if $cur_status[0] eq $status;
    rename "$dir/$cur_status[0]", "$dir/$status"
        or die "Can't set status for feed $self->{'@'}: $!";
    return $cur_status[0];
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
sub clobber   { $_[0]->{'clobber'}   }

sub dir         { $_[0]->{'_dir'}        }
sub source      { $_[0]->{'_source'}     }
sub channels    { @{ $_[0]->{'_channels'} } }

sub files {
    my ($self) = @_;
    my $dir = $self->dir;
    return $self->{'_fileskv'}->elements;
}

sub _shadow {
    my ($self, $file) = @_;
    my $dir = $self->dir . '/shadow';
    my $n = 4;
    my ($rand, $shadow);
    -d $dir or mkdir $dir or die "Can't mkdir $dir $!";
    while (defined($rand = $self->_random_hex(8)) && !link $file, $shadow = "$dir/shadow.$rand") {
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
