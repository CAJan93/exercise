This is a basic scala project that includes spark dependencies and necessary build-configuration to build a jar that can be submitted to a spark installation via spark-submit.
Things to do after checking out and opening in intellij:
    Add scala SDK 2.11.11 (tested with this version, it has to be 2.11.x since this is the spark version)
    Add jdk 8
    add Framework support of Maven
To build the project:
    run mvn package

Start the project using the command
    mvn compile & mvn deploy & cp -a /TPCH/. /target/ ; java -jar target/SparkTutorial-1.0.jar
    The position of your TPCH folder might differ
    or
    mvn compile ; mvn deploy ; java -jar target/SparkTutorial-1.0.jar --cores 4