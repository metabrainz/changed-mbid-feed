#!/usr/bin/perl
use 5.10.0;
use List::Util qw( sum );
use LWP::UserAgent;
use Time::HiRes qw( gettimeofday tv_interval );

my $lwp = LWP::UserAgent->new;
my @intervals;

for my $i (0..30) {
    my $t0 = [ gettimeofday ];
    $lwp->get('http://0.0.0.0:8000/since/65527');
    push @intervals, tv_interval($t0);
}

say sum(@intervals) / $#intervals;
