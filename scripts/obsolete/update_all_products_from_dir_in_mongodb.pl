#!/usr/bin/perl -w

# This file is part of Product Opener.
#
# Product Opener
# Copyright (C) 2011-2020 Association Open Food Facts
# Contact: contact@openfoodfacts.org
# Address: 21 rue des Iles, 94100 Saint-Maur des Fossés, France
#
# Product Opener is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

#use Modern::Perl '2017';
use utf8;

use Storable qw(lock_store lock_nstore lock_retrieve);
use Encode;
use MongoDB;
use Data::Dumper;

my $timeout = 60000;
my $database = "off";
#my $collection = "products_revs";
my $collection = "products_revs";

my $products_collection = MongoDB::MongoClient->new->get_database($database)->get_collection($collection);

my $start_dir = $ARGV[0];

my %errStatus;
my $verbose = 1;

my $d = 0;
my @products = ();

# functions

sub Log {
  $message = shift;
  print "$message\n" if $verbose;
  if ($message=~/^\[(...)\]/) {
    $errStatus{$message}++ if $1 ne "INF"
  }
} 

sub retrieve {
        my $file = shift @_;
        # If the file does not exist, return undef.
        if (! -e $file) {
                return;
        }
        my $return = undef;
        eval {$return = lock_retrieve($file);};

        return $return;
}

sub get_path_from_code($) {

        my $code = shift;
        my $path = "";

        # Require at least 4 digits (some stores use very short internal barcodes, they are likely to be conflicting)
        if ($code !~ /^\d{4,24}$/) {

                Log("[WAR] code should be at least 4 digits maximum 24: $code");
        }

        if ($code =~ /^(...)(...)(....?)(.*)$/) {
                $path = "$1/$2/$3/$4";
                $path=~s/\/$//
        } else {
                Log("[WAR] code has not a correct format: $code")
        }
        return $path;
}


sub find_products {

        my $dir = shift;
        my $code = shift;

        my $dh;

        opendir $dh, "$dir" or die "could not open $dir directory: $!\n";
        foreach my $file (sort readdir($dh)) {
                chomp($file);
                if ($file =~ /^(([0-9]+))\.sto/) {
                        push @products, [$code, $1];
                        $d++;
                        (($d % 1000) == 1 ) and Log("[INF] identified $d revisions - $code");
                }
                else {
                        $file =~ /\./ and next;
                        if (-d "$dir/$file") {
                                find_products("$dir/$file","$code$file");
                        }
                }
                #last if $d > 100;
        }
        closedir $dh or die "cannot not close $dir: $!\n";

        return
}

# main begins

if (not defined $start_dir) {
        Log("[ERR] Pass the root of the product directory as the first argument");
        exit();
}

if (scalar $#products < 0) {
        find_products($start_dir,'');
}

my $count = $#products;
my $i = 0;

my $previousChangesFile = "";
my %changes;
my %originalChanges;
my $productFile;

my %codes = ();

Log("[INF] total is $count revisions to update");

foreach my $code_rev_ref (@products) {
  my ($code, $rev) = @$code_rev_ref;

  my $path = get_path_from_code($code);

  if ($path eq "") {
    Log("[WAR] skipping product: $code rev: $rev");
    next
  }

  $productFile = "$start_dir/$path/$rev.sto";

  my $product_ref = retrieve($productFile);

  if ($product_ref) {

    $changesFile = "$start_dir/$path/changes.sto";

    if ($changesFile eq $previousChangesFile) {
      #print "[INF] knowned changes file\n"

    } else {
      #print "[INF] identified a new changes file\n";
      $originalChanges = retrieve($changesFile);
      if ($originalChanges) {
        $previousChangesFile = $changesFile;
        %changes = ();
        foreach my $change (@$originalChanges) {
          #print "\n>>>>>>>>>>>>>>>>>>>>>>>>>>>\n";
          #print Dumper $change;
          #print "\n<<<<<<<<<<<< $$change{rev} <<<<<<<<<<<<<<<<<<<<<\n";
          if ($$change{"rev"}) {
            $k = $$change{"rev"};
            $changes{$k} = $change
          } else {
            $errMsg = "[WAR] no rev in change record see $changesFile";
            #print "$errMsg\n";
            $errStatus{$errMsg}++
          }
        }
              
      } else {
        Log("[WAR] $changesFile not defined");
        $previousChangesFile = ""
      }
    }
    
    #print "\n>>>>>>>>>>>>>>>>>>>>\n";
    #print Dumper(\%changes);
    #print "\n<<<<<<<<<<<<<<<<<<<<<<\n";
    next if ((defined $product_ref->{deleted}) and ($product_ref->{deleted} eq 'on'));

    #Log("[INF] updating product code $code -- rev $rev -- " . $product_ref->{code});

    $product_ref->{"_id"} = $code . "." . $rev;

    if ($changes{$rev}{"rev"} and $product_ref->{"rev"} and $changes{$rev}{"rev"} eq $product_ref->{"rev"}) {
      $$product_ref{"change"} = $changes{$rev};
      #print Dumper($product_ref)
    } else {
      Log("[WAR] cannot match any change from changes file");
    }

    Log("[INF] updating $productFile db record (key ".$product_ref->{"_id"}."), loop $i");

    # this hack works for first level hash but not nested ones
    #foreach my $key (my @for_deleting_while_iterating =  keys $product_ref) {
    #  if ($key=~/\./) {
    #    Log("[WAR] renaming $key key as it contains a dot");
    #    delete $$product_ref{$key}
    #  }
    #}

    my $str = Dumper($product_ref);

    # we replace dot by something else
    $str=~s/'([^']+)\.([^']+)' =>/'$1_$2' =>/g;

    # regexp below to eval str
    $str=~s/\$VAR1/\%product_ref2/;
    $str=~s/= \{/= (/;
    $str=~s/\};\s*$/);/;
    #print($str);
    eval($str);

    #$products_collection->update_one({"_id" => $product_ref2->{_id}}, $product_ref2, { upsert => 1 });
    $products_collection->insert_one(\%product_ref2);
    $i++;
    $codes{$code} = 1

  } else {
    Log("[ERR] cannot find $productFile")
  }
}

Log("[INF] $count products revs to update - $i products revs not empty or deleted");
Log("[INF] scalar keys codes : " . (scalar keys %codes));

print Dumper(\%errStatus);

exit(0);
