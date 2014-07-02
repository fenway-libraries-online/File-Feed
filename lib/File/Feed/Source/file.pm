package File::Feed::Source::file;

use strict;
use warnings;

use vars qw(@ISA);

@ISA = qw(File::Feed::Source);

use File::Feed::Source;
use File::Feed::File;
use File::Copy qw(copy move);

sub protocol { 'file' }

sub feed { $_[0]->{'_feed'} }
sub destination { $_[0]->{'destination'} }

sub begin {
    my ($self, $feed) = @_;
    my $root = $self->root;
    die "No such directory: $root" if !-d $root;
    return $self;
}

sub end { }

sub id { $_[0]->{'#'} }

sub list {
    my ($self, $from, $recursive) = @_;
    goto &rlist if $recursive;
    my $root = $self->root;
    my $dir = defined $from ? "$root/$from" : $root;
    my $ofs = length($dir) + 1;
    my @files = glob("$dir/*");
    return grep { -f $_ ? (substr($_, $ofs)) : () } @files;
}

sub rlist {
    my ($self, $from) = @_;
    my $root = $self->root;
    my $dir = "$root/$from";
    my $ofs = length($dir) + 1;
    my @files = glob("$dir/*");
    my @list;
    _crawl($from, \@list);
    return @list;
}

sub _crawl {
    my ($from, $list) = @_;
    my @files = glob("$from/*");
    foreach (@files) {
        if (-d $_) {
            _crawl($from, $list);
        }
        else {
            push @$list, $_;
        }
    }
}

sub fetch {
    my ($self, $from, $to) = @_;
    if ($self->{'copy'}) {
        copy($from, $to);
    }
    else {
        move($from, $to);
    }
}

sub basename {
    (my $path = shift) =~ s{^.+/}{};
    return $path;
}

1;

=pod

=head1 NAME

File::Feed::Source::file - fetch files from a filesystem

=cut

