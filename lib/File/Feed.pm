package File::Feed;

use strict;
use warnings;

use File::Kvpar;
use File::Kit;
use File::Feed::Channel;
use File::Feed::Source;
use File::Basename qw(basename);

use vars qw($VERSION);

$VERSION = '0.01';

# Feed statuses
use constant EMPTY   => '@empty';
use constant FILLING => '@filling';
use constant FULL    => '@full';
use constant ERROR   => '@error';
use constant FROZEN  => '@frozen';

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
    if ($self->status(FILLING)) {
        my $dir  = $self->dir;
        my %logged = map { $_->{'to'} => $_ } $self->files;
        $source->begin($self);
        foreach my $chan (@chan) {
            my ($from_dir, $to_dir, $filter, $recursive) = ($chan->from, $chan->to, $chan->filter, $chan->recursive);
            foreach ($source->list($from_dir, $recursive)) {
                (my $path = $_) =~ s{^$from_dir/}{};
                my ($from, $to) = ($_, "$to_dir/$path");
                next if !$filter->($path);
                next if $logged{$to};
                if ($source->fetch($from, "$dir/files/$to")) {
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
    $self->status(@files ? FULL : EMPTY);
    return @files;
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
    return $status if $cur_status[0] eq $status;
    rename "$dir/$cur_status[0]", "$dir/$status"
        or die "Can't set status for feed $self->{'@'}: $!";
    return $status;
}

sub id          { $_[0]->{'#'}           }
sub host        { $_[0]->source->host    }
sub root        { $_[0]->source->root    }
sub from        { $_[0]->{'from'} || $_[0]->id }
sub to          { $_[0]->{'to'} || $_[0]->id }

sub description { $_[0]->{'description'} }
sub user        { $_[0]->{'user'}        }
sub dir         { $_[0]->{'_dir'}        }
sub source      { $_[0]->{'_source'}     }
sub channels    { @{ $_[0]->{'_channels'} } }

sub files {
    my ($self) = @_;
    my $dir = $self->dir;
    return $self->{'_fileskv'}->elements;
}

1;

=pod

=head1 NAME

File::Feed - gather files from an FTP server or other source

=cut
