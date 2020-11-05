package File::Feed::Source::sftp;

use strict;
use warnings;

use vars qw(@ISA);

sub usage;

@ISA = qw(File::Feed::Source);

use File::Feed::Source;
use File::Feed::File;
use Net::SFTP::Foreign;
use Net::Netrc;
use Fcntl qw(S_ISDIR);

sub protocol { 'sftp' }

sub feed { $_[0]->{'_feed'} }

sub key_file { $_[0]->{'key-file'} || $_[0]->{'key_file'} }

sub begin {
    my ($self, $feed) = @_;
    my $host = $self->host;
    my $user = $self->user;
    my $key_file = $self->key_file;
    my $pass = $self->password;
    my @credentials;
    if (defined $key_file) {
        usage "only a key file *or* a password may be specified, not both"
            if defined $pass;
        @credentials = ('key_path' => $key_file);
    }
    elsif (defined $pass) {
        @credentials = ('password' => $pass);
    }
    else {
        my $mach = Net::Netrc->lookup($host) || die "Can't determine password for $host";
        my $login;
        ($login, $pass) = $mach->lpa;
        die "Looking for $user at $host, found $login" if defined $user && $login ne $user;
    }
    my $root = $self->root;
    my $err;
    my $client = Net::SFTP::Foreign->new(
        'host' => $host,
        'user' => $user,
        @credentials,
        'autodie' => 0,
    );
    $self->_check("Can't login: %s");
    $self->_check($client->setcwd($root), "Can't cwd $root: %s");
    @$self{qw(_client _feed _list)} = ($client, $feed, {});
    return $self;
}

sub _check {
    my $self = shift;
    my $msg = pop;
    my $client = $self->{'_client'};
    my $err = $client->error;
    die sprintf($msg, $err) if $err;
}

sub end {
    my ($self) = @_;
    delete @$self{qw(_client _feed _list)};
    return $self;
}

sub list {
    my ($self, $path, $recursive) = @_;
    my $root = $self->root;
    my $abspath = defined $path ? "$root/$path" : $root;
    my $ofs = length($abspath) + 1;
    my $client = $self->{'_client'};
    my $dir = $client->ls($abspath) or return;
    $self->_check("Couldn't retrieve a list of files in $path");
    my @files;
    foreach (@$dir) {
        my ($name, $attr) = @$_{qw(filename a)};
        my $file = "$abspath/$name";
        if (!S_ISDIR($attr->{'perm'})) {
            push @files, $file;
        }
    }
    return map { substr($_, $ofs) } @files;
}

sub oldlist {
    my ($self, $path, $recursive) = @_;
    goto &rlist if $recursive;
    $path = '.' if !defined $path;
    my $client = $self->{'_client'};
    my $list = $client->ls($path)
        or die "Can't list $path: ", $client->message;
    return @$list;
}

sub rlist {
    my ($self, $path) = @_;
    my $client = $self->{'_client'};
    my @list;
    _crawl($client, $path, \@list);
    return @list;
}

sub _crawl {
    my ($client, $path, $list) = @_;
    my $dir = $client->dir($path) or return;
    foreach my $line (@$dir) {
        next if $line !~ /^([-d])[-a-z]{9}\s.+\s(\S+)$/;
        my ($type, $file) = ($1, "$path/$2");
        if ($type eq 'd') {
            _crawl($client, $file, $list);
        }
        else {
            push @$list, $file;
        }
    }
}

sub fetch_file {
    my ($self, $from, $to) = @_;
    my $client = $self->{'_client'};
    $self->_check($client->get($from, $to), "Can't fetch $from: %s");
}

sub basename {
    (my $path = shift) =~ s{^.*/}{};
    return $path;
}

1;

=pod

=head1 NAME

File::Feed::Source - fetch files from an FTP server

=cut

