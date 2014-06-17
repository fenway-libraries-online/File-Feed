package File::Feed::Source::ftp;

use strict;
use warnings;

use File::Feed::File;

use base 'File::Feed::Source';

use Net::FTP;
use Net::Netrc;

sub protocol { 'ftp' }

sub feed { $_[0]->{'_feed'} }

sub begin {
    my ($self, $feed) = @_;
    my $host = $self->host;
    my $user = $self->{'user'}     ||= $self->{'uri'}->user;
    my $pass = $self->{'password'} ||= $self->{'uri'}->password;
    if (!defined $pass) {
        my $mach = Net::Netrc->lookup($host) || die "Can't determine password for $host";
        my $login;
        ($login, $pass) = $mach->lpa;
        die "Looking for $user at $host, found $login" if defined $user && $login ne $user;
    }
    my $root = $self->root;
    my $client = Net::FTP->new($host) or die "Can't connect";
    $client->login($user, $pass)      or die "Can't login: ",     $client->message;
    $client->cwd($self->root)         or die "Can't cwd $root: ", $client->message;
    @$self{qw(_client _feed _list)} = ($client, $feed, {});
    return $self;
}

sub end {
    my ($self) = @_;
    delete @$self{qw(_client _feed _list)};
    return $self;
}

sub list {
    my ($self, $from, $recursive) = @_;
    goto &rlist if $recursive;
    my $client = $self->{'_client'};
    my $list = $client->ls($from)
        or die "Can't list $from: ", $client->message;
    return @$list;
}

sub rlist {
    my ($self, $from) = @_;
    my $client = $self->{'_client'};
    my @list;
    _crawl($client, $from, \@list);
    return @list;
}

sub _crawl {
    my ($client, $from, $list) = @_;
    my $dir = $client->dir($from) or return;
    foreach my $line (@$dir) {
        next if $line !~ /^([-d])[-a-z]{9}\s.+\s(\S+)$/;
        my ($type, $name) = ($1, $2);
        my $path = "$from/$name";
        if ($type eq 'd') {
            _crawl($client, $path, $list);
        }
        else {
            push @$list, $path;
        }
    }
}

sub fetch {
    my ($self, $from, $to) = @_;
    my $client = $self->{'_client'};
    $client->get($from, $to)
        or die "Can't get $from: ", $client->message;
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

