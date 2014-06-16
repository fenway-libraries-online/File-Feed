package File::Feed::Channel;

use strict;
use warnings;

use Text::Glob qw(glob_to_regex);

sub new {
    my $cls = shift;
    bless { @_ }, $cls;
}

sub id { $_[0]->{'#'} }
sub from { $_[0]->{'from'} || $_[0]->{'in'} || $_[0]->{'#'} }
sub to { $_[0]->{'to'} || $_[0]->{'in'} || $_[0]->{'#'} }
sub recursive { $_[0]->{'recursive'} }
sub autodir { $_[0]->{'autodir'} }
sub description { $_[0]->{'description'} }

sub filter {
    my ($self) = @_;
    return $self->{'_filter'} if $self->{'_filter'};
    my $spec = $self->{'filter'} or return $self->{'_filter'} = sub { 1 };
    if ($spec =~ s/^pcre://) {
        my $rx = qr/$spec/;
        return $self->{'_filter'} = sub { shift() =~ $rx };
    }
    elsif ($spec =~ s/^glob://) {
        my $rx = _pattern2regexp($spec);
        return $self->{'_filter'} = sub { shift() =~ $rx };
    }
    else {
        die "Unknown filter: $spec";
    }
}

sub regexp {
    my ($self) = @_;
    $self->{'_regexp'} ||= _pattern2regexp($self->{'match'} || '*');
}

sub _pattern2regexp {
    my ($pat) = @_;
    return qr/./ if !defined $pat;
    return qr/$1/ if $pat =~ m{^/(.+)/$};
    return glob_to_regex($pat);
}

1;

