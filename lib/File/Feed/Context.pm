package File::Feed::Context;

use strict;
use warnings;

use String::Expando;

sub new {
    my $cls = shift;
    die if @_ % 2;
    my %stash;
    while (@_ >= 2) {
        my ($key, $val) = splice @_, 0, 2;
        if (ref($val) eq 'HASH') {
            %stash = ( %stash, %$val, $key => $val );
        }
        else {
            %stash = ( %stash, $key => $val );
        }
    }
    bless {
        'stash' => \%stash,
        'expando' => String::Expando->new,
    }, $cls;
}

sub expand {
    my ($self, $str) = @_;
    return $self->{'expando'}->expand($str, $self->{'stash'});
}

1;
