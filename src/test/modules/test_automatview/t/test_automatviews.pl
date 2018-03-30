use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
use Path::Tiny;

sub runTests {
    my $pgNode = get_new_node('master');
    $pgNode->init;
    $pgNode->start;

    my @setupFiles = getFilesByExtension('setup', 'sql');
    my @allTestQueries = getFilesByExtension('test_queries', 'sql');
    my $numberOfTests = (scalar @allTestQueries) * 2;
    plan tests => $numberOfTests;
    
    foreach my $setupFile (@setupFiles) {
        my $filePath = path($setupFile) or die "Can't open file $!";
        my $databaseName = extractDatabaseNameFromFileName($filePath->basename);
        my $query = $filePath->slurp or die "Can't open file: $!";
        my $queryOutput = $pgNode->safe_psql('postgres', $query);
        
        executeQueriesForDatabase($pgNode, $databaseName);
        $pgNode->restart;
    }
}

sub executeQueriesForDatabase {
    my $pgNode = shift;
    my $database = shift;

    my @queryFiles = getFilesByExtension('test_queries', "$database\.sql");

    foreach my $queryFile (@queryFiles) {
        my $queryFilePath = path($queryFile) or die "Can't open file $!";
        my $query = $queryFilePath->slurp;

        my @expectedOutputs = getExpectedOutputsForQueryFile(
            $queryFilePath->basename, $database);
        my $expectedMatViewCount = $expectedOutputs[0];
        my $expectedMatViewQuery = $expectedOutputs[1];
        my $queryStderr = executeQuery($pgNode, $database, $query);

        my $actualMatViewCount = extractMatViewCount($queryStderr);
        my $actualMatViewQuery = extractMatViewQuery($queryStderr);
        is($actualMatViewCount, $expectedMatViewCount,
            "Number of created MatViews should match for queries in $queryFile");
        is($actualMatViewQuery, $expectedMatViewQuery,
            "Rewritten query should match expected rewrite in $queryFile");
    }
}

sub getExpectedOutputsForQueryFile {
    my $queryFileName = shift;
    my $database = shift;

    my $expectedOutputFile = getExpectedFileForQueryFile($queryFileName, $database);
    my $expectedOutputFilePath = path($expectedOutputFile);
    my $expectedOutput = $expectedOutputFilePath->slurp or
        die "Failed to read expected output file: $expectedOutputFile";
    my @splitExpectedOutput = split(/\n/, $expectedOutput);
    my $expectedMatViewCount = $splitExpectedOutput[0];
    my $expectedMatViewQuery = $splitExpectedOutput[1];

    return @splitExpectedOutput;
}

sub extractMatViewCount {
    my $queryOutput = shift;

    my @queryLines = split(/\n/, $queryOutput);
    my @matchingLines = grep(/Pruned to/, @queryLines);
    my $numMatches = @matchingLines;

    if (scalar $numMatches == 1 && $matchingLines[0] =~ m/Pruned to (\d+) MatViews/) {
        return $1
    } elsif ($numMatches == 0) {
        return "0";
    } else {
        die "extractMatViewCount expected zero or one matching line";
    }

    return undef;
}

sub extractMatViewQuery {
    my $queryOutput = shift;

    my @queryLines = split(/\n/, $queryOutput);
    my @matchingLines = grep(/Rewritten query:/, @queryLines);
    my $numMatches = @matchingLines;

    if ($numMatches == 1 && $matchingLines[0] =~ m/Rewritten query: (.*)$/) {
        return simplifyMatViewName($1);
    } elsif ($numMatches == 0) {
        return "null";
    } else {
        die "extractMatViewQuery expected only one matching line";
    }

    return undef;
}

sub simplifyMatViewName {
    my $queryString = shift;

    (my $simplifiedQuery = $queryString) =~ s/Matview_[0-9]+/Matview/g;
    return $simplifiedQuery;
}

sub executeQuery {
    my $pgNode = shift;
    my $database = shift;
    my $query = shift;

    my ($stdout, $stderr);

    say STDERR "Executing query:\n$query";
    my $queryOutput = $pgNode->psql(
        'postgres', $query,
        stdout        => \$stdout,
        stderr        => \$stderr,
        on_error_die  => 1,
        on_error_stop => 0);
    say STDERR "Query test stdout: $stdout";
    say STDERR "Query test stderr: $stderr";

    return $stderr;
}

sub getExpectedFileForQueryFile {
    my $queryFile = shift;
    my $database = shift;
    my @expectedOutputFiles = getFilesByExtension('expected', "$database\.out");

    my @splitQueryFileName = split(/\//, $queryFile);
    my @queryFileName = $splitQueryFileName[1];    
    my $fileExtensionIndex = rindex($queryFile, '.');
    my $matchingOutputFileName = substr($queryFile, 0, $fileExtensionIndex) . '.out';


    foreach my $outputFile (@expectedOutputFiles) {
        my $outputFileName = path($outputFile)->basename;
        if ($outputFileName eq $matchingOutputFileName) {
            return $outputFile;
        }
    }

    return undef;
}

sub getFilesByExtension {
    my $directory = shift;
    my $fileExtension = shift;

    my @filelist;
    my $file;

    opendir(DIR, $directory) or die "can't opendir $directory: $!";
    while (defined($file = readdir(DIR))) {
        if ($file =~ m/.*\.$fileExtension/) {
            unshift(@filelist, "$directory/$file")
        }
    }
    closedir(DIR);

    return @filelist;
}

sub extractDatabaseNameFromFileName {
    my $fileName = shift;

    my @splitFileName = split(/\./, $fileName);
    return $splitFileName[1];
}

runTests();