package File::Feed::Sink::kit;

use strict;
use warnings;

use vars qw(@ISA);

@ISA = qw(File::Feed::Sink);

use File::Feed::Sink;
use File::Kit;

sub protocol { 'kit' }

sub path { $_[0]->{'path'} ||= $_[0]->{'uri'}->path }

sub begin {
    my ($self) = @_;
    my $path = $self->path;
    $self->{'_kit'} = File::Kit->create($path);
    return $self;
}

sub end {
    my ($self) = @_;
    $self->{'_kit'}->save;
    return $self;
}

sub store {
    my $self = shift;
    my $kit = $self->{'_kit'};
    foreach my $file (@_) {
        $kit->add($file->path, $file);
    }
}

1;
