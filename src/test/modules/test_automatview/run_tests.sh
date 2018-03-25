OUTFILE="out.txt"
TEST_DB="test_db"
TEST_DIR="test_queries"
EXPECTED_DIR="expected"
PG_DATA_FILE="../../../../../pg_data"

runTestQueries() {
    if [ -d "$TEST_DIR" ]
        then
            startPostgres

            for queryFile in $TEST_DIR/*.sql; do
                echo "Found queryFile: $queryFile"
                fileName=$(cut -d "/" -f 2 <<< "$queryFile")
                testFileName="$(cut -d "." -f 1 <<< "$fileName").out"
                testFilePath="$EXPECTED_DIR/$testFileName"

                if [ -f "$testFilePath" ]
                    then
                        echo "runTestQueries getting test file from $testFilePath"
                        expectedMatViewCount=$(head -n 1 "$testFilePath")
                        expectedQuery=$(tail -n 1 "$testFilePath")
                        runTest "$queryFile" "$expectedMatViewCount" "$expectedQuery"
                        restartPostgres
                    else echo "runTestQueries couldn't find expected test file: $testFilePath"
                fi
            done

            stopPostgres
        else echo "runTestQueries couldn't find test directory: $TEST_DIR"
    fi
}

startPostgres() {
    echo "Starting postgres..."
    ../../../../../pgsql/bin/pg_ctl start -D "$PG_DATA_FILE" &> /dev/null
}

restartPostgres() {
    echo "Restarting postgres..."
    ../../../../../pgsql/bin/pg_ctl restart -D "$PG_DATA_FILE" &> /dev/null
}

stopPostgres() {
    echo "Stopping postgres"
    ../../../../../pgsql/bin/pg_ctl stop -D "$PG_DATA_FILE" &> /dev/null
}

runTest() {
    if [ -z "$1" && -z "$2" && -z "$3" ]
        then
            echo "usage: runTests sqlFile expectedMatViewCount expectedQuery"
        else
            sqlFile="$1"
            expectedMatViewCount="$2"
            expectedQuery="$3"

            echo "----- Running test for $sqlFile -----"
            executeSqlFile "$sqlFile" "$TEST_DB"
            isExpectedMatViewCount "$expectedMatViewCount"
            isExpectedQuery "$expectedQuery"
            echo "----- Finished Test -----"
    fi
}

executeSqlFile() {
    sqlFile=$1
    database=$2
    if [ -f "$sqlFile" ]
        then ../../../../../pgsql/bin/psql -f "$sqlFile" -d "$database" &> "$OUTFILE"
        else echo "Could not locate file: $sqlFile"
    fi
}

isExpectedMatViewCount() {
    expectedCount=$1
    if [ -f "$OUTFILE" ]
        then
            matViewCountLine=$(grep "Pruned to" "$OUTFILE")
            actualCount=$(echo "$matViewCountLine" | gawk 'match($0, /Pruned to ([0-9]+) MatViews/, a) { print a[1] }')
            checkExpected "MatViewCount" "$expectedCount" "$actualCount"
        else
            echo "Could not locate test output file: $OUTFILE"
            return 0
    fi
}

isExpectedQuery() {
    expectedQuery="$1"
    if [ -f $OUTFILE ]
        then
            queryLine=$(grep "Rewritten query:" "$OUTFILE")
            query=$(echo "$queryLine" | gawk 'match($0, /Rewritten query: (.*)$/, a) { print a[1] }')
            simplifiedQuery=$(simplifyQueryMatViewName "$query")
            checkExpected "Query Test" "$expectedQuery" "$simplifiedQuery"
        else
            echo "Could not locate test output file: $OUTFILE"
            return 0
    fi
}

simplifyQueryMatViewName() {
    if [ -z "$1" ]
        then
            echo "usage: simplifyQueryMatViewName query"
        else
            query=$1
            echo $(sed -E "s/Matview_[0-9]+/Matview/g" <<< "$query")
    fi
}

checkExpected() {
    testName="$1"
    expected="$2"
    actual="$3"

    if [ "$expected" == "$actual" ]
        then echo "$testName passed"
        else printf "$testName expected:\n$expected\nFound:\n$actual\n"
    fi
}

runTestQueries
