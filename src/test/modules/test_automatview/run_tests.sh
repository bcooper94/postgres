OUTFILE="out.txt"
TEST_DB="test_db"
TEST_DIR="test_queries"
EXPECTED_DIR="expected"
PG_DATA_FILE="../../../../../pg_data"
NULL="null"

runTestQueries() {
    if [ -d "$TEST_DIR" ]
        then
            startPostgres

            for queryFile in $TEST_DIR/*.sql; do
                fileName=$(cut -d "/" -f 2 <<< "$queryFile")
                testFileName="$(cut -d "." -f 1 <<< "$fileName").out"
                testFilePath="$EXPECTED_DIR/$testFileName"

                if [ -f "$testFilePath" ]
                    then
                        echo "runTestQueries getting test file from $testFilePath"
                        expectedMatViewCount=$(head -n 1 "$testFilePath")
                        expectedQuery=$(tail -n 1 "$testFilePath")
                        if [ "$expectedQuery" == "$NULL" ] # tail won't pick up a blank line
                            then
                                expectedQuery=""
                        fi
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
    ../../../../../pgsql/bin/pg_ctl start -D "$PG_DATA_FILE" &> /dev/null

    if [ $? != 0 ]
        then echo "Failed to start postgres. Exiting..."
        exit 1
    fi
}

restartPostgres() {
    ../../../../../pgsql/bin/pg_ctl restart -D "$PG_DATA_FILE" &> /dev/null
    if [ $? != 0 ]
        then echo "Failed to restart postgres. Exiting..."
        exit 1
    fi
}

stopPostgres() {
    ../../../../../pgsql/bin/pg_ctl stop -D "$PG_DATA_FILE" &> /dev/null
    if [ $? != 0 ]
        then echo "Failed to stop postgres. Exiting..."
        exit 1
    fi
}

runTest() {
    sqlFile="$1"
    expectedMatViewCount="$2"
    expectedQuery="$3"

    echo "----- Running test for $sqlFile -----"
    executeSqlFile "$sqlFile" "$TEST_DB"
    isExpectedMatViewCount "$expectedMatViewCount"
    isExpectedQuery "$expectedQuery"
    echo "----- Finished Testing $sqlFile -----"
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
    query=$1
    echo $(sed -E "s/Matview_[0-9]+/Matview/g" <<< "$query")
}

checkExpected() {
    testName="$1"
    expected="$2"
    actual="$3"

    if [ "$expected" != "$actual" ]
        then printf "ERROR: $testName expected:\n\"$expected\"\nFound:\n\"$actual\"\n"
    fi
}

runTestQueries
