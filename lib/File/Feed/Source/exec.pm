package File::Feed::Source::exec;

use strict;
use warnings;

use vars qw(@ISA);

@ISA = qw(File::Feed::Source);

use File::Feed::Source;
use Cwd qw(cwd);
use Text::ParseWords qw(shellwords);

sub protocol { 'exec' }

sub feed { $_[0]->{'_feed'} }
sub destination { $_[0]->{'destination'} }

sub begin {
    my ($self, $feed) = @_;
    $self->{'_feed'} = $feed;
    return $self;
}

sub end { }

sub id { $_[0]->{'#'} }

sub fetch {
    my ($self, %arg) = @_;
    my ($channel, $dest) = @arg{qw(channel destination)};
    my $feed = $self->feed;
    my $path = $channel->local_path;
    $dest .= "/$path" if defined $path;
    my @cmd = shellwords($channel->{'command'} or die 'No command for channel ', $channel->id);
    my $expando = String::Expando->new;
    my $ctx = $feed->context(
        'channel' => $channel,
        'source'  => $self,
    );
    my $outfile = $expando->expand($channel->{'output-file'}, $ctx);
    my $cwd = cwd;
    chdir $dest or die "Can't chdir $dest: $!";
    my $ok;
    eval {
        open my $fhout, '>', $outfile or die "Can't open $outfile: $!";
        open my $fhcmd, '-|', @cmd    or die "Can't run @cmd: $!";
        copy($fhcmd, $fhout)          or die "Can't output";
        $ok = 1;
    };
    if (!$ok) {
        print STDERR $@, "\n";
        chdir $cwd or die "Can't chdir $cwd: $!";
        exit 2;
    }
}

sub spawn {
    my @cmd = @_;
    my $pid = fork;

}

