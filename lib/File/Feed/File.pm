package File::Feed::File;

use strict;
use warnings;

sub new {
    my $cls = shift;
    my %arg = @_;
    $arg{'#'} ||= delete $arg{'id'};
    $arg{'@'} ||= 'file';
    bless \%arg, $cls;
}

sub id { $_[0]->{'#'} }
sub from { $_[0]->{'from'} }
sub to { $_[0]->{'to'} }
sub channel { $_[0]->{'_channel'} }

1;
