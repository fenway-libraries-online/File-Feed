package File::Feed::Source::file;

use strict;
use warnings;

sub protocol { 'file' }

sub feed { $_[0]->{'_feed'} }
sub destination { $_[0]->{'destination'} }

sub begin {
    my ($self, $feed) = @_;
    $self->{'context'} = {
        'feed' => $feed,
        'listed'  => {},
    };
    $client->cwd("$root/$src") or die;
    return $self;
}

sub id { $_[0]->{'#'} }

sub list {
    my ($self, $chan) = @_;
    my $root = $self->root;
    my $ctx = $self->{'context'};
    my ($feed, $list) = @$ctx{qw(feed listed)};
    my $filter = $chan->filter;
    my $from = $chan->from;
    (my $dir = "$root/$from") =~ s{/\.$}{};
    my $n = length($srcdir) + 1;
    my @files = map { substr($_, $n) } grep { $_ =~ $rx } glob("$srcdir/*");
    $self->{'listed'} = map {
        $_ => File::Feed::File->new(
            '@' => 'file',
            '#' => "$srcdir/$_",
        ),
    } @files;
    return values %{ $self->{'listed'} };
}

sub fetch {
    my $self = shift;
    my $chan = shift;
    my $feed = $self->feed;
    my $src  = $chan->source;
    my $dst  = $chan->destination || $src;
    my $rx   = $chan->regexp;
    my $root = $feed->root;
    my $dir  = $feed->dir;
    my @fetched;
    my %logged = map { $_->{'source'} => 1 } $feed->files;
    my $client = $self->{'_client'} || $self->connect->{'_client'};
    my @all_files = $client->ls;
    my @new_files = grep { $_ =~ $rx && !$logged{"$root/$src/$_"} } @all_files;
    foreach my $file (@new_files) {
        $client->get("$src/$file", "$dir/files/$dst/$file")
            or die "Can't get $src/$file: ", $client->message;
        push @fetched, {
            '_channel' => $chan,
            'file' => "$dst/$file",
            'source' => "$root/$src/$file",
            'fetch_time' => time,
        }
    }
    return @fetched;
}

sub basename {
    (my $path = shift) =~ s{^.+/}{};
    return $path;
}

1;

=pod

=head1 NAME

File::Feed::Source - fetch files from an FTP server

=cut

