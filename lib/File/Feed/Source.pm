package File::Feed::Source;

use strict;
use warnings;

use URI;

sub new {
    my ($cls, $uri) = @_;
    $uri = URI->new($uri) if !ref $uri;
    my $scheme = $uri->scheme;
    $cls .= '::' . lc $scheme;
    eval "use $cls; 1" or die $@;
    bless {
        'uri'  => $uri,
        'host' => $uri->host,
        'user' => $uri->user,
        'password' => $uri->password,
        'root' => $uri->path,
    }, $cls;
}

sub uri { $_[0]->{'uri'} }
sub host { $_[0]->{'host'} }
sub root { $_[0]->{'root'} }

1;
