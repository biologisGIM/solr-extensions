# solr-extensions
Currently a set of extensions to the streaming expression functionality

FieldStreamOperations provides for the users two new functionalities 
with field operations while using SolrCloud:

Two new functions
    - 'AddField' creates new custom field with the value
    Example:
    addField(value=35.99, as=discount)
    Example's output:
    "discount":"35.99"

    - 'MergeValue' merge the custom values to the values of the existing fields
    Example:
    mergeValue(field=discount, delim=",", mergeValue=EUR)
    Example's output:
    "discount":"35.99,EUR"

Prerequisites:

1. Preferable IDE or TextEditor
2. Java Development Kit (JDK) 12
- JDK 8 will works to but it will requires changes in 'pom.xml' file*
3. Apache Maven
4. Apache Solr
5. Make sure you're connected to the Internet

The JDK can be downloaded from Oracle's Official Page:
https://www.oracle.com/technetwork/java/javase/downloads/

Maven (Java dependency manager)
- for this project being used Maven version 3.6.1

The Maven can be downloaded from Apache Maven Project official Page:
https://maven.apache.org/download.cgi
(we need a binary only, like: apache-maven-3.6.1-bin.zip)

The Solr can be downloaded from the Apache-Solr official Page:
https://lucene.apache.org/solr/mirrors-solr-latest-redir.html
(for this plugins being used solr-7.7.0)

Environment Setup:

1. Install JDK by running JDK installer
2. Extract apache-maven from the archive and move to preferable location
3. optional: to make Maven commands globally accessible (from command line),
please add the following line to '/.bash_profile' file:

    export PATH=$PATH:/{DIRECTORY}/apache-maven-3.6.1/bin

where {DIRECTORY} location of apache-maven folder

4. Save the '/.bash_profile' and source it running the command:

    source ~/.bash_profile

5. Extract the downloaded Solr archive in a preferable location

Building the project:

1. In a command line navigate to the folder 'AddField'(or 'MergeValue')
- inside you'll see following:
    - AddField.iml (MergeValue.iml)
    - out         
    - pom.xml      
    - src          
    - target

2. To build the project run following command:
    
    mvn compile

    - alternatively you can use:

    mvn install
    (this command install all dependencies, compile and test the project)

3. To test the project run following command:

    mvn test

    In order to see the test reports, navigate to target/surefire-reports/ folder.
    There you'll see following documents:
    - com.biologis./.../.txt (a very brief report)
    - TEST-com.biologis./.../.xml
    

4. To create jar file which could be used for as a plugin for SolrCloud, run following command:

    mvn package

    - in target folder you'll see the StreamOperations-1.0-SNAPSHOT.jar file

Streaming expression plugin implementation to SolrCloud:

1. Navigate to solr's cintrib directory:
    /solr-{VERSION}/solr/contrib

2. Create a folder named 'plugins' for your custom-made plugins

3. Copy the generated jar file from your target directory to the plugins directory

4. Navigate to 'configsets' directory:
    /solr-{VERSION}/solr/server/solr/configsets

5. Copy and paste the existing 'sample_techproducts_configs' directory and rename it
(for example techproducts_test)

6. Navigate to the file solrconfig.xml
    /solr-{VERSION}/.../.../techproducts_test/conf/solrconfig.xml

7. Add following lines into solrconfig.xml file

    <lib dir="${solr.install.dir:../../../..}/contrib/plugins" regex=".*\.jar" />
        (this line will navigate solr to the location of the plugins)

    <expressible name="addField" class="com.biologis.jars.AddField"/>
    <expressible name="mergeValue" class="com.biologis.jars.MergeValue"/>
        (these two lines are makes our plugins available from the SolrCloud 'Stream section')
    
8. Save the changes in solrconfig.xml file

Running the SolrCloud and performing the new Streaming Expressions

1. (In a command line) navigate to the Solr directory:
    /solr-{VERSION}/solr/

2. In order to create a new 'techproducts_test' collection run the following command:

    bin/solr create -c techproducts -s 2 -rf 2 -d server/solr/configsets/techproducts_test/conf -n techproducts_test

    bin/solr start -c -p 8983 -s example/cloud/node1/solr
    bin/solr start -c -p 7574 -s example/cloud/node2/solr -z localhost:9983

    (ports 8983, 7574 & 9983 are given by default; but you can also customize the by assigning your own)

3. Indexing the data. In the command line run the following command:

    bin/post -c techproducts example/exampledocs/*
    (this will take all the data from exampledocs directory and index them)

Working in the SolrCloud Environment

1. Run the following link in your web-browser (this link was tested on Safari and Chrome):

    http://localhost:8983/

2. In your browser navigate to collection (choose 'techproducts') and then select 'Stream' section

3. Since the AddField and MergeValue are Streaming Operations they should be wrapped by other Streaming Expressions like 'select()'
Example:

select(
    search(techproducts, q="cat:electronics", fl="id, manu, inStock, price, price_c", sort="id asc"),
    id,
    manu,
    inStock,
    price,
    price_c,
    addField(value=35.85, as=discount),
    mergeValue(field=discount, delim=",", mergeValue="EUR")
)

in the output you'll see newly generated field: {"discount": "35.85,EUR"}, which is basically a result of both
(AddField and MergeValue) functions.

Last but not least. Make sure that the functions (in Streaming expression window) starts with the small letter:
    addField(value=..., as=...),
    mergeValue(field=..., delim="...", mergeValue=...)

* in order to use JDK 8 please change the following lines in pom.xml file:
                
                <configuration>
                    <source>12</source>
                    <target>12</target>
                </configuration>

    by replacing 12 on 1.8