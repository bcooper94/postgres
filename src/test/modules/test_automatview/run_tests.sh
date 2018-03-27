OUTFILE="out.txt"
SETUP_DIR="setup"
TEST_DIR="test_queries"
EXPECTED_DIR="expected"
PG_DATA_FILE="../../../../../pg_data"
NULL="null"

runTestSuite() {
    if [ -d "$SETUP_DIR" ]
        then
            startPostgres
            for setupFile in $SETUP_DIR/*.sql; do
                fileName=$(getFileNameFromPath "$setupFile")
                database=$(getTargetDatabaseFromFileName "$fileName")
                createDatabase "$database" "$setupFile"
                runTestQueriesForDatabase "$database"
                dropDatabase "$database"
                restartPostgres
            done
            stopPostgres
        else
            echo "Could not find database setup directory: $SETUP_DIR"
            exit 1
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
        then echo "Failed to restart postgres. Stopping Postgres and exiting..."
        stopPostgres
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

createDatabase() {
    database="$1"
    tableCreationScript="$2"
    echo "Creating database: \"$database\" from script: \"$tableCreationScript\""
    ../../../../../pgsql/bin/createdb "$1" &> /dev/null

    if [ $? != 0 ]
        then
            echo "Failed to create database $database. Stopping Postgres and exiting..."
            stopPostgres
            exit 1
    fi

    executeSqlFile "$tableCreationScript" "$database"
}

dropDatabase() {
    ../../../../../pgsql/bin/dropdb "$1" &> /dev/null

    if [ $? != 0 ]
        then echo "Failed to drop database $1. If this database exists, you will need to drop it manually."
    fi
}

runTestQueriesForDatabase() {
    if [ -d "$TEST_DIR" ]
        then
            database="$1"

            for queryFile in $TEST_DIR/*.$database.sql; do
                fileName=$(getFileNameFromPath "$queryFile")
                testFileName="$(sed 's/\(.*\)\..*/\1/' <<< "$fileName").out"
                testFilePath="$EXPECTED_DIR/$testFileName"

                if [ -f "$testFilePath" ]
                    then
                        expectedMatViewCount=$(head -n 1 "$testFilePath")
                        expectedQuery=$(tail -n 1 "$testFilePath")
                        if [ "$expectedQuery" == "$NULL" ] # tail won't pick up a blank line
                            then
                                expectedQuery=""
                        fi
                        runTest "$database" "$queryFile" "$expectedMatViewCount" "$expectedQuery"
                        restartPostgres
                    else echo "runTestQueriesForDatabase couldn't find expected test file: $testFilePath"
                fi
            done
        else echo "runTestQueriesForDatabase couldn't find test directory: $TEST_DIR"
    fi
}

getFileNameFromPath() {
    cut -d "/" -f 2 <<< "$1"
}

getTargetDatabaseFromFileName() {
    cut -d "." -f 2 <<< "$1"
}

getExtensionFromFileName() {
    cut -d "." -f 3 <<< "$1"
}

runTest() {
    database="$1"
    sqlFile="$2"
    expectedMatViewCount="$3"
    expectedQuery="$4"

    echo "----- Running test for $sqlFile -----"
    executeSqlFile "$sqlFile" "$database"
    isExpectedMatViewCount "$expectedMatViewCount"
    isExpectedQuery "$expectedQuery"

    if [ -f "$OUTFILE" ]
        then rm "$OUTFILE"
    fi

    echo "----- Finished Testing $sqlFile -----"
}

executeSqlFile() {
    sqlFile=$1
    database=$2
    if [ -f "$sqlFile" ]
        then ../../../../../pgsql/bin/psql -f "$sqlFile" -d "$database" &> "$OUTFILE"
            if [ $? != 0 ]
                then
                    echo "Failed to execute SQL file: $sqlFile. Exiting..."
                    exit 1
            fi
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

runTestSuite
