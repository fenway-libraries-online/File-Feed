package File::Feed::Util;

use Text::Glob qw(glob_to_regex);

sub pat2rx {
    my ($pat) = @_;
    return qr/.?/ if !defined $pat;
    return qr/$pat/ if $pat =~ s/^(pcre|regexp)://;
    return qr/$1/ if $pat =~ m{^/(.+)/$};
    $pat =~ s/^glob://;
    return glob_to_regex($pat);
}

1;
