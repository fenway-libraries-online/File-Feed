package File::Feed;

use strict;
use warnings;

use File::Kvpar;
use File::Kit;
use File::Feed::Source;
use File::Feed::Channel;
use File::Feed::Sink;
use File::Feed::Util;
use File::Path qw(mkpath);
use File::Copy qw(move copy);
use File::Basename qw(basename dirname);
use String::Expando;

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
    my @elems = $kv->elements;
    my ($feed, $source, @etc);
    ($feed,   @etc) = grep { $_->{'@'} eq 'feed'    } @elems; die if !defined $feed   || @etc;
    ($source, @etc) = grep { $_->{'@'} eq 'source'  } @elems; die if !defined $source || @etc;
    my @sinks       = grep { $_->{'@'} eq 'sink'    } @elems;
    my @channels    = grep { $_->{'@'} eq 'channel' } @elems;
    if ($feed->{'perl-class'}) {
        $cls = $feed->{'perl-class'};
        eval "use $cls; 1" or die "Can't use feed class $cls: $!";
    }
    @channels = ({
        '@' => 'channel',
        '#' => 'default',
        'from' => '.',
        'to' => '.',
        'description' => 'default channel',
        'filter' => 'glob:*',
    }) if !@channels;
    my $self = bless {
        '_dir' => $dir,
        %$feed,
        '_feedkv' => $kv,
        '_fileskv' => File::Kvpar->new('+<', "$dir/files.kv"),
        '_random_buf' => '',
    }, $cls;
    $self->{'_source'  } = $self->_source_instance($source);
    $self->{'_channels'} = [ map { $self->_channel_instance(%$_) } @channels ];
    $self->{'_sinks'   } = [ map { $self->_sink_instance(%$_)    } @sinks    ];
    return $self;
}

sub _source_instance {
    my $self = shift;
    return File::Feed::Source->new(@_);
}

sub _sink_instance {
    my $self = shift;
    return File::Feed::Sink->new(@_);
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
    my ($self, %arg) = @_;
    # What channels should we look at?
    my %chan = map { $_->id => 1 } $self->channels(@{ $arg{'channels'} || [] });
    # Within those channels, what files are new?
    my $new_dir = $self->path('new');
    my @new;
    _crawl($new_dir, \@new);
    s{^$new_dir/}{} for @new;
    my %is_new  = map { $_ => 1 } @new;
    # Instantiate all files meeting those criteria
    return map {
               $self->_file_instance(%$_)
           }
           grep {
               $is_new{$_->{'to'}}
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

sub _group_files {
    my ($self, $stash, @files) = @_;
    my $expando = String::Expando->new;
    my $now = strftime('%Y%m%dT%H%M%S', localtime);
    my ($ymd, $hms) = split 'T', $now;
    my %group;
    foreach my $file (@files) {
        my $chan = $file->channel;
        my $c = $chan->id;
        my %ctx = (
            %$self,
            %$chan,
            'date' => $ymd,
            'time' => $hms,
            'datetime' => $now,
            %$stash,
            %$file,
        );
        my $sink  = $ctx{'sink'};
        $sink = "file:dropoff/" if !defined $sink;
        my $g = $expando->expand($sink, {
            %ctx,
            'feed'    => $self,
            'channel' => $chan,
            'file'    => $file,
        });
        $group{$g} ||= {
            'files' => [],
        };
        push @{ $group{$g}{'files'} }, $file;
    }
    return values %group;
}

sub fill {
    my ($self, %arg) = @_;
    my $source = $self->source;
    my @new;
    my $ok = eval {
        # Start filling
        $self->status(FILLING) or die "Can't set status: not a feed?";
        my %logged = map { $_->{'to'} => $_ } $self->files;
        $source->begin($self);
        my @chan = $self->channels(@{ $arg{'channels'} || [] });
        foreach my $chan (@chan) {
            my $ctx = $self->context(
                'source' => $source,
                'channel' => $chan,
            );
            push @new, $source->fetch($ctx, \%logged);
        }
        1;
    };
    $self->_cleanup;
    $self->status($ok ? IDLE : ERROR);
    return @new;
}

sub drain {
    my ($self, %arg) = @_;
    my @new = $self->new_files(%arg);
    return if !@new;
    my @files;
    my $ok = eval {
        # Start draining
        $self->status(DRAINING) or die "Can't set status: not a feed?";
        my %uri2files;
        foreach my $file (@new) {
            my $channel = $file->channel;
            my $ctx = $self->context(
                'source'  => $channel->source,
                'channel' => $channel,
                'file'    => $file,
            );
            my $uri = $ctx->expand($file->destination);
            push @{ $uri2files{$uri} ||= [] }, $file;
        }
        while (my ($uri, $files) = each %uri2files) {
            my $sink = $self->_sink_instance($uri);
            $sink->store(@$files);
            push @files, @$files;
        }
        1;
    };
    $self->_cleanup;
    $self->status($ok ? IDLE : ERROR);
    return @files;
}

sub oldfill {
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

sub olddrain {
    my $self = shift;
    ### unshift @_, 'to' if @_ % 2;
    my %arg = @_;
    ### my $to_kit = delete $arg{'kit'};
    ### my $to_dir = delete $arg{'to'};
    my @new = $self->new_files(%arg);
    return if !@new;
    ### my ($nxnum, $nxchr) = (1, 'a');
    ### my %file_stash = (
    ### );
    ### my %group_stash = (
    ###     %file_stash,
    ###     'nxnum' => sub { $nxnum++ },
    ###     'nxchr' => sub { $nxchr++ },
    ### );
    my $sink = $self->_sink_instance('uri' => $self->{'sink'});
    my @groups;
    my %stash;
    my $ok = eval {
        # Start draining
        $self->status(DRAINING) or die "Can't set status: not a feed?";
        @groups = $self->_group_files(\%stash, @new);
        foreach my $group (@groups) {
            $sink->store($group);
        }
        1;
    };
    $self->_cleanup;
    $self->status($ok ? IDLE : ERROR);
    die "Drain failed: $@\n" if !$ok;
    return @groups;
    ### {
    ###     my $autodir = $self->autodir;
    ###     # Prepare to group files
    ###     my ($channels, $strip, $clobber) = @arg{qw(channels strip clobber)};
    ###     my $group_by = defined $arg{'group-by'} ? $arg{'group-by'} : $self->{'group-by'} || '.';
    ###     my %group;
    ###     # Assign files to (a) group(s)
    ###     foreach my $file (@new) {
    ###         my $g = $file->{'group'} ||= defined($group_by) ? $expando->expand($group_by, { %file_stash, $file }) : '.';
    ###         my $group = $group{$g} ||= {
    ###             '@' => 'group',
    ###             '#' => $g,
    ###             '_files' => [],
    ###         };
    ###         push @{ $group->{'_files'} ||= [] }, $file;
    ###     }
    ###     my @out;
    ###     my %have_dir;
    ###     foreach my $group (values %group) {
    ###         my $files = delete $group->{'_files'};
    ###         if (defined $to_kit) {
    ###             my $path = $expando->expand($to_kit, { %group_stash, %$group });
    ###             my $kit = $self->_kit_instance(
    ###                 'path' => $path,
    ###                 'meta' => {
    ###                     '#'    => $group->{'#'},
    ###                     'feed' => $self->id,
    ###                 },
    ###                 'move' => sub { link $_[0], $_[1] },
    ###             );
    ###             $kit->add(@$files);
    ###             push @out, $kit;
    ###         }
    ###         elsif (defined $to_dir) {
    ###             my $to = $expando->expand($to_dir, { %group_stash, %$group });
    ###             foreach my $file (@$files) {
    ###                 my $path = my $dest_path = $file->to;
    ###                 if ($strip) {
    ###                     my $n = $strip;
    ###                     $dest_path =~ s{^[^/]+/}{} while $n--;
    ###                 }
    ###                 my ($new, $dest) = ($self->path('new', $path), "$to/$dest_path");
    ###                 my $dest_dir = dirname($dest);
    ###                 if (!$have_dir{$dest_dir}++ && !-d $dest_dir) {
    ###                     die "Destination directory $dest_dir does not exist"
    ###                         if !$arg{'autodir'};
    ###                     mkpath($dest_dir);
    ###                 }
    ###                 if (-e $dest) {
    ###                     if ($arg{'clobber'}) {
    ###                         print STDERR "File $dest already exists, clobbering it\n";
    ###                         unlink $dest;
    ###                     }
    ###                     else {
    ###                         print STDERR "File $dest already exists, skipping $new\n";
    ###                         next;
    ###                     }
    ###                 }
    ###                 $self->_shadow($new);
    ###                 move($new, $dest) or die "Can't move $path to $dest_dir: $!";
    ###                 push @out, $to;
    ###             }
    ###         }
    ###         elsif ($r eq 'CODE') {
    ###             foreach my $file (@new) {
    ###                 $self->_shadow($self->path('new', $file->path));
    ###                 $to->($file);
    ###             }
    ###         }
    ###     }
    ###     1;
    ### };
    ### $self->_cleanup;
    ### $self->status($ok ? IDLE : ERROR);
    ### die "Fill failed: $@\n" if !$ok;
}

sub path {
    my $self = shift;
    return join '/', $self->dir, @_;
}

sub assemble {
    my ($self, %arg) = @_;
    my @new = $self->new_files(%arg);
    my %kit;
    foreach (@new) {
        my $kitname = $_->{'kit'} || '.';
        push @{ $kit{$kitname} ||= [] }, $_;
    }
    while (my ($kitname, $files) = each %kit) {
        $kitname = $self->_random_hex(8) if !length $kitname;
        my $path = $self->path('pickup', $kitname . '.kit');
        my $kit = $self->_kit_instance(
            'path' => $path,
            'meta' => {
                '#'    => $kitname,
                'feed' => $self->id,
            }
        );
        $_->{'kit'} ||= $kitname for @$files;
        $kit->add(@$files);
        $kit->save;
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
    my %chan;
    foreach my $spec (@_) {
        my $rx = File::Feed::Util::pat2rx($spec);
        %chan = ( %chan, map { my $c = $_->id; $c =~ $rx ? ($c => $_) : () } @chan );
    }
    return values %chan;
    #return grep { $chan{$_->id} } @chan;
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
