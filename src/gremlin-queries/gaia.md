0. codepath:

    https://github.com/sighingnow/GraphScope/tree/htap/gaia-runner

1. start vineyardd

    vineyardd --socket=/tmp/vineyard.sock --meta=local

2. loading modern graph

    ./bin/arrow_fragment_test /tmp/vineyard.sock 1 "$HOME/gstest/modern_graph/knows.csv#header_row=true&src_label=person&dst_label=person&label=knows&delimiter=|" 1 "$HOME/gstest/modern_graph/person.csv#header_row=true&label=person&delimiter=|"

3. generate query plan

3.0: build

    cd htap/src/research/query_service/gremlin/compiler
    mvn package -DskipTests=true

3.1. start compiler

    java -cp ".:gremlin-server-plugin/target/gremlin-server-plugin-1.0-SNAPSHOT-jar-with-dependencies.jar:gremlin-server-plugin/target/classes" com.alibaba.graphscope.gaia.GremlinServiceMain

3.2. generate query plan

    export HTAP_ROOT=.....

    java -cp .:./benchmark-tool/target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool $HTAP_ROOT/src/gremlin-queries/tpcc/tpcc_queries ".txt" $HTAP_ROOT/src/gremlin-queries/tpcc/tpcc_plans ".plan" 0.0.0.0 8182

    java -cp .:./benchmark-tool/target/benchmark-tool-1.0-SNAPSHOT-jar-with-dependencies.jar com.alibaba.graphscope.gaia.GeneratePlanBinaryTool $HTAP_ROOT/src/gremlin-queries/ldbc/queries ".txt" $HTAP_ROOT/src/gremlin-queries/ldbc/query_plans ".plan" 0.0.0.0 8182

4. run query

4.1. export query plan

    export QUERY_PLAN=$HOME/gsa/research/query_service/gremlin/compiler/conf/modern_plans/both_step_test_01.plan

4.2. run query

    ./target/debug/gaia_runner --graph-name modern --partition-num 1 --worker-id 0 --graph-vineyard-object-id 1678889428296062

