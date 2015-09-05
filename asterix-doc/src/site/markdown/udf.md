# Support for User Defined Functions in AsterixDB #

## <a id="#toc">Table of Contents</a> ##
* [Writing an External UDF](#WritingAnExternalUDF)
* [Creating an AsterixDB Library](#CreatingAnAsterixDBLibrary)
* [Installing an AsterixDB Library](#installingUDF)
* [Using UDF to preprocess feed-collected data](#PreprocessingCollectedData)

In this document, we describe the support for implementing, using, and installing user-defined functions (UDF) in
AsterixDB. We will explain how we can use UDFs to preprocess, e.g., data collected using 
feeds (see the [feeds tutorial](feeds/tutorial.html)).

A feed definition may optionally include the specification of a
user-defined function that is to be applied to each feed record prior
to persistence. Examples of pre-processing might include adding
attributes, filtering out records, sampling, sentiment analysis, feature
extraction, etc. We can express a UDF, which can be defined in AQL or in a programming
language such as Java, to perform such pre-processing. An AQL UDF is a good fit when
pre-processing a record requires the result of a query (join or aggregate)
over data contained in AsterixDB datasets. More sophisticated
processing such as sentiment analysis of text is better handled
by providing a Java UDF. A Java UDF has an initialization phase
that allows the UDF to access any resources it may need to initialize
itself prior to being used in a data flow. It is assumed by the
AsterixDB compiler to be stateless and thus usable as an embarrassingly
parallel black box. In contrast, the AsterixDB compiler can
reason about an AQL UDF and involve the use of indexes during
its invocation.

## <a id="WritingAnExternalUDF">Writing an External UDF</a> ###

A Java UDF in AsterixDB is required to implement an interface. We shall next write a basic UDF that extracts the 
hashtags contained in the tweet's text and appends each into an unordered list. The list is added as an additional
attribute to the tweet to form the augment version - ProcessedTweet`.

    package org.apache.asterix.external.library;

    import org.apache.asterix.external.library.java.JObjects.JDouble;
    import org.apache.asterix.external.library.java.JObjects.JPoint;
    import org.apache.asterix.external.library.java.JObjects.JRecord;
    import org.apache.asterix.external.library.java.JObjects.JString;
    import org.apache.asterix.external.library.java.JObjects.JUnorderedList;
    import org.apache.asterix.external.library.java.JTypeTag;
    import org.apache.asterix.external.util.Datatypes;

    public class AddHashTagsFunction implements IExternalScalarFunction {

        private JUnorderedList list = null;
        private JPoint location = null;

        @Override
        public void initialize(IFunctionHelper functionHelper) {
            list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
            location = new JPoint(0, 0);
        }

        @Override
        public void deinitialize() {
        }

        @Override
        public void evaluate(IFunctionHelper functionHelper) throws Exception {
            list.clear();
            JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
            JString text = (JString) inputRecord.getValueByName(Datatypes.Tweet.MESSAGE);
            JDouble latitude = (JDouble) inputRecord.getValueByName(Datatypes.Tweet.LATITUDE);
            JDouble longitude = (JDouble) inputRecord.getValueByName(Datatypes.Tweet.LONGITUDE);

            if (latitude != null && longitude != null) {
                location.setValue(latitude.getValue(), longitude.getValue());
            }
            String[] tokens = text.getValue().split(" ");
            for (String tk : tokens) {
                if (tk.startsWith("#")) {
                    JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
                    newField.setValue(tk);
                    list.add(newField);
                }
            }

            JRecord outputRecord = (JRecord) functionHelper.getResultObject();
            outputRecord.setField(Datatypes.Tweet.ID, inputRecord.getValueByName(Datatypes.Tweet.ID));

            JRecord userRecord = (JRecord) inputRecord.getValueByName(Datatypes.Tweet.USER);
            outputRecord.setField(Datatypes.ProcessedTweet.USER_NAME,
                    userRecord.getValueByName(Datatypes.Tweet.SCREEN_NAME));

            outputRecord.setField(Datatypes.ProcessedTweet.LOCATION, location);
            outputRecord.setField(Datatypes.Tweet.CREATED_AT, inputRecord.getValueByName(Datatypes.Tweet.CREATED_AT));
            outputRecord.setField(Datatypes.Tweet.MESSAGE, text);
            outputRecord.setField(Datatypes.ProcessedTweet.TOPICS, list);

            inputRecord.addField(Datatypes.ProcessedTweet.TOPICS, list);
            functionHelper.setResult(outputRecord);
        }

    }

A Java UDF has an associated factory class that is required and is used by AsterixDB in creating an instance of the function at runtime. Given below is the corresponding factory class.

    package org.apache.asterix.external.library;

    import org.apache.asterix.external.library.IExternalScalarFunction;
    import org.apache.asterix.external.library.IFunctionFactory;

    public class AddHashTagsFunctionFactory implements IFunctionFactory {

        @Override
        public IExternalScalarFunction getExternalFunction() {
            return new AddHashTagsFunction();
        }
    }

At this stage, we shall compile the above two source files. To do so, we would need the following jars.

    asterix-common-0.8.7-SNAPSHOT.jar
    asterix-external-data-0.8.7-SNAPSHOT.jar

## <a id="CreatingAnAsterixDBLibrary">Creating an AsterixDB Library</a> ###

We need to install our Java UDF so that we may use it in AQL statements/queries. An AsterixDB library has a pre-defined structure which is as follows.
	

 - A **jar** file, which contains the class files for your UDF source code. 

 - File `descriptor.xml`, which is a descriptor with meta-information about the library.

	    <externalLibrary xmlns="library">
    		<language>JAVA</language>
    		<libraryFunctions>
    			<libraryFunction>
    				<function_type>SCALAR</function_type>
    				<name>addHashTags</name>
    				<arguments>Tweet</arguments>
    				<return_type>ProcessedTweet</return_type>
    				<definition>org.apache.asterix.external.library.AddHashTagsFunctionFactory
    				</definition>
    			</libraryFunction>
    		</libraryFunctions>
    	</externalLibrary>


- lib: other dependency jars

If the Java UDF requires additional dependency jars, you may add them under a "lib" folder is required. 

We create a zip bundle that contains the jar file and the library descriptor xml file. The zip would have the following structure.

	$ unzip -l ./tweetlib.zip 
	Archive:  ./tweetlib.zip

        Length     Date   Time    Name
        --------    ----   ----    ----
        760817  04-23-14 17:16   hash-tags.jar
        405     04-23-14 17:16   tweet.xml
        --------                   -------
        761222                   2 files
        
### <a name="installingUDF">Installing an AsterixDB Library</a>###

We assume you have followed the [installation instructions](../install.html) to set up a running AsterixDB instance. Let us refer your AsterixDB instance by the name "my_asterix".

- Step 1: Stop the AsterixDB instance if it is in the ACTIVE state.

   		$ managix stop -n my_asterix
    

- Step 2: Install the library using Managix install command. Just to illustrate, we use the help command to look up the syntax

	    $ managix help  -cmd install
    	Installs a library to an asterix instance.
    	Options
    	n  Name of Asterix Instance
    	d  Name of the dataverse under which the library will be installed
    	l  Name of the library
    	p  Path to library zip bundle
	

Above is a sample output and explains the usage and the required parameters. Each library has a name and is installed under a dataverse. Recall that we had created a dataverse by the name - "feeds" prior to  creating our datatypes and dataset. We shall name our library - "testlib".

We assume you have a library zip bundle that needs to be installed.
To install the library, use the Managix install command. An example is shown below.

	$ managix install -n my_asterix -d feeds -l testlib -p <put the absolute path of the library zip bundle here> 

You should see the following message:

	INFO: Installed library testlib

We shall next start our AsterixDB instance using the start command as shown below.

	$ managix start -n my_asterix

You may now use the AsterixDB library in AQL statements and queries. To look at the installed artifacts, you may execute the following query at the AsterixDB web-console.

	for $x in dataset Metadata.Function 
	return $x

	for $x in dataset Metadata.Library	
	return $x

Our library is now installed and is ready to be used.

## <a id="PreprocessingCollectedData">Preprocessing Collected Data</a> ###

In the following we assume that you already created the `TwitterFeed` and its corresponding 
data types and dataset following the instruction explained in the [feeds tutorial](feeds/tutorial.html).

We consider an example transformation of a raw tweet into its
lightweight version called `ProcessedTweet`, which is defined next. 

        use dataverse feeds;

        create type ProcessedTweet if not exists as open {
            id: string,
            user_name:string,
            location:point,
            created_at:string,
            message_text:string,
            country: string,
            topics: {{string}}
        };

        create dataset ProcessedTweets(ProcessedTweet)
        primary key id;        
        
The processing required in transforming a collected tweet to its lighter version of type `ProcessedTweet` involves extracting the topics or hash-tags (if any) in a tweet
and collecting them in the referred "topics" attribute for the tweet.
Additionally, the latitude and longitude values (doubles) are combined into the spatial point type. Note that spatial data types are considered as first-class citizens that come with the support for creating indexes. Next we show a revised version of our example TwitterFeed that involves the use of a UDF. We assume that the UDF that contains the transformation logic into a "ProcessedTweet" is available as a Java UDF inside an AsterixDB library named 'testlib'. We defer the writing of a Java UDF and its installation as part of an AsterixDB library to a later section of this document. 

        use dataverse feeds;

        create feed ProcessedTwitterFeed if not exists
        using "push_twitter"
        (("type-name"="Tweet"),
        ("consumer.key"="************"),  
        ("consumer.secret"="**************"),
        ("access.token"="**********"),  
        ("access.token.secret"="*************"))
        
        apply function testlib#addHashTagsInPlace;

Note that a feed adaptor and a UDF act as pluggable components. These
contribute towards providing a generic "plug-and-play" model where
custom implementations can be provided to cater to specific requirements.

####Building a Cascade Network of Feeds####
Multiple high-level applications may wish to consume the data
ingested from a data feed. Each such application might perceive the
feed in a different way and require the arriving data to be processed
and/or persisted differently. Building a separate flow of data from
the external source for each application is wasteful of resources as
the pre-processing or transformations required by each application
might overlap and could be done together in an incremental fashion
to avoid redundancy. A single flow of data from the external source
could provide data for multiple applications. To achieve this, we
introduce the notion of primary and secondary feeds in AsterixDB.

A feed in AsterixDB is considered to be a primary feed if it gets
its data from an external data source. The records contained in a
feed (subsequent to any pre-processing) are directed to a designated
AsterixDB dataset. Alternatively or additionally, these records can
be used to derive other feeds known as secondary feeds. A secondary
feed is similar to its parent feed in every other aspect; it can
have an associated UDF to allow for any subsequent processing,
can be persisted into a dataset, and/or can be made to derive other
secondary feeds to form a cascade network. A primary feed and a
dependent secondary feed form a hierarchy. As an example, we next show an 
example AQL statement that redefines the previous feed
"ProcessedTwitterFeed" in terms of their
respective parent feed (TwitterFeed).

        use dataverse feeds;
		
        drop feed ProcessedTwitterFeed if exists;

        create secondary feed ProcessedTwitterFeed from feed TwitterFeed 
        apply function testlib#addHashTags;

The `addFeatures` function is already provided in the release.  Later
in the tutorial we will explain how this function or
other functions can be added to the system.