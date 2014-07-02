package File::Feed::Sink;

use strict;
use warnings;

use URI;

sub new {
    my $cls = shift;
    my %arg;
    if (@_ == 1 && ref($_[0]) eq 'HASH') {
        %arg = %{ shift() };
    }
    elsif (@_ % 2) {
        %arg = ( 'uri', @_ );
    }
    else {
        %arg = @_;
    }
    my $uri = $arg{'#'} || $arg{'uri'} or die "Can't instantiate a sink without a URI";
    $uri = 'file://' . $uri if $uri =~ m{^/};
    $uri = URI->new($uri) if !ref $uri;
    my $scheme = $uri->scheme;
    $cls .= '::' . lc $scheme;
    eval "use $cls; 1" or die $@;
    bless {
        'uri'  => $uri,
        #'host' => $uri->host,
        #'user' => $uri->user,
        #'password' => $uri->password,
        #'root' => $uri->path,
    }, $cls;
}

sub uri { $_[0]->{'uri'} }
sub host { $_[0]->{'host'} }
sub root { $_[0]->{'root'} }

1;

