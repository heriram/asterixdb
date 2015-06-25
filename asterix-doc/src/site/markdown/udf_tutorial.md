# CREATING A USER-DEFINED FUNCTION TUTORIAL

##What's in the Document?

In this document, we describe and study the support for data feed ingestion in AsterixDB, a Big Data Management System (BDMS) that provides a platform for the scalable storage, searching, and analysis of very large volumes of semi-structured data. Data feeds are a mechanism for having continuous data arrive into a data management system from external sources that produce data continuously, and to have that data incrementally populate a persisted dataset and associated indexes. The need to persist and index "fast-flowing" high-velocity data (and support ad hoc analytical queries) is ubiquitous. However the state of that art today involves 'gluing' together different systems. AsterixDB is different in being a unified system with "native support" for data feed ingestion. AsterixDB allows end-user to define a datafeed and have its data be persisted and indexed into an AsterixDB dataset via the use of AQL statements. The document walks the reader through the basic concepts involved in defining and using a data feed. A sample application is used to illustrate the usage of data feeds.

**Pre-Requisites:** This document assumes that you have a running AsterixDB instance.

##Tutorial

The AsterixDB query language (AQL) has built-in support for data feeds. To illustrate the end-user may model of a data feed, we consider a basic application that retrieves tweet from the Twitter service (using the Twitter API). In Twitter's convention, tweets often contain hashtags (words beginning with a #) that are symbolic of the topics associated with the tweet, as chosen by the author. The application intends to persist (and index) an augmented version of each tweet that has the list of associated topics as an additional attribute. The pre-processing of tweets to form the augmented version is implemented as a Java UDF. AsterixDB allows the end-user to provide a UDF that is used to pre-process in-flight tweets before persistence. Such a UDF (together with any dependencies) is packaged as an AsterixDB library. The AsterixDB lifecycle management tool - Managix allows the end-user to install such a library, subsequent to which the contained functions may be used in AQL statements/queries. Note that an AsterixDB library may also contain custom adaptors to fetch from an external source of choice.

##Building our Application

AsterixDB uses its proprietary data model, hereafter referred to as ADM (AsterixDB Data Model). ADM is a super-set of JSON. We begin by forming an ADM representation of a tweet. Each tweet contains information about the associated user account. The information is present as an embedded (nested) user data type. Given below are the AQL statements that create a datatype TwittertUser that represents a Twitter account. The TwitterUser datatype is used in the definition of a Tweet that follows.

We begin by creating a dataverse, that acts like a holder or a namespace for the datatype(s), dataset(s) and feed definition we shall create.

	use dataverse feeds;

	create type TwitterUser  if not exists
	as open{
	screen_name: string,
	language: string,
	friends_count: int32,
	status_count: int32,
	name: string,
	followers_count: string
	};

	create type Tweet if not exists as open{
	id: string,
	user:TwitterUser,
	latitude:double,
	longitude:double,
	created_at:string,
	message_text:string
	};

	create dataset Tweets(Tweet) if not exists
	primary key id;

As mentioned earlier, our application needs to form an augmented version of a tweet that includes the list of topics (hashtags) found in each tweet. We refer to the augmented version as ProcessedTweet. Next we define a ProcessedTweet as an ADM datatype.

	create type ProcessedTweet if not exists as open {
	id: string,
	user_name:string,
	latitude:double,
	longitude:double,
	created_at:string,
	message_text:string,
	country: string,
	topics: [string]
	};

Note the the ProcessedTweet datatype includes an unordered list of strings denoted by the attribute "topics". Let us now define the dataset where we shall persist the tweets.

	create dataset ProcessedTweets(ProcessedTweet) if not exists
	primary key id;
	
Above, we have defined a dataset Tweets with each contained record conforming with the datatype - ProcessedTweet.

##Pre-Processing of Data (Tweets)

A Java UDF in AsterixDB is required to implement an prescribe interface. We shall next write a basic UDF that extracts the hashtags contained in the tweet's text and appends each into an unordered list. The list is added as an additional attribute to the tweet to form the augment version - ProcessedTweet.

	package edu.uci.ics.asterix.external.library;

	import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
	import edu.uci.ics.asterix.external.library.java.JObjects.JString;
	import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
	import edu.uci.ics.asterix.external.library.java.JTypeTag;

	public class HashTagsFunction implements IExternalScalarFunction {

	    private JUnorderedList list = null;

	    @Override
	    public void initialize(IFunctionHelper functionHelper) {
	        list = new JUnorderedList(functionHelper.getObject(JTypeTag.STRING));
	    }

	    @Override
	    public void deinitialize() {
	    }

	    @Override
	    public void evaluate(IFunctionHelper functionHelper) throws Exception {
	        list.clear();
	        JRecord inputRecord = (JRecord) functionHelper.getArgument(0);
	        JString text = (JString) inputRecord.getValueByName("message_text");

	        // extraction of hashtags
	        String[] tokens = text.getValue().split(" ");
	        for (String tk : tokens) {
	            if (tk.startsWith("#")) {
	                JString newField = (JString) functionHelper.getObject(JTypeTag.STRING);
	                newField.setValue(tk);
	                list.add(newField);
	            }
	        }

	        // forming the return value - an augmented tweet with an additional attribute - topics
	        JRecord result = (JRecord) functionHelper.getResultObject();
	        result.setField("id", inputRecord.getFields()[0]);
	        result.setField("user_name", inputRecord.getFields()[1]);
	        result.setField("latitude", inputRecord.getFields()[2]);
	        result.setField("longitude", inputRecord.getFields()[3]);
	        result.setField("created_at", inputRecord.getFields()[4]);
	        result.setField("message_text", inputRecord.getFields()[5]);
	        result.setField("topics", list);
	        functionHelper.setResult(result);
	    }

	}
A Java UDF has an associated factory class that is required and is used by AsterixDB in creating an instance of the function at runtime. Given below is the corresponding factory class.

	package edu.uci.ics.asterix.external.udf;

	import edu.uci.ics.asterix.external.library.IExternalScalarFunction;
	import edu.uci.ics.asterix.external.library.IFunctionFactory;

	public class HashTagsFunctionFactory implements IFunctionFactory {

	    @Override
	    public IExternalScalarFunction getExternalFunction() {
	        return new HashTagsFunction();
	    }
	}

At this stage, we shall compile the above two source files. To do so, we would need the following jars.

	asterix-common-0.8.6-SNAPSHOT.jar
	asterix-external-data-0.8.6-SNAPSHOT.jar

You may create an eclipse project , create the above source files in the required packages and include the dependencies in the build path. 
Alternatively, you may download the eclipse project bundle from here​. 
You would need to un-tar the bundle and import the contents as an eclipse project.
Creating an AsterixDB Library

We need to install our Java UDF so that we may use it in AQL statements/queries.
An AsterixDB library has a pre-defined structure which is as follows.

- **jar** file containing all class files. 
This is the jar file that would contain the class files for your UDF source code. In the case of our application, it will include the class files for the function and associated factory.

- **library descriptor.xml**
This is a descriptor that provide meta-information about the library.

<xml>
	
	<externalLibrary xmlns="library">	
		<language>JAVA</language>
		<libraryFunctions>
			<libraryFunction>
				<function_type>SCALAR</function_type>
				<name>hashTags</name>
				<arguments>Tweet</arguments>
				<return_type>ProcessedTweet</return_type>
				<definition>edu.uci.ics.asterix.external.udf.HashTagsFunctionFactory
				</definition>
			</libraryFunction>
		</libraryFunctions>
	</externalLibrary>
</xml>


- **lib/<other dependency jars>**

If the Java UDF requires additional dependency jars, you may add them under a "lib" folder is required. The UDF in our application does not have any dependency jars and so we need not have the lib directory in our library bundle.

We create a zip bundle that contains the jar file and the library descriptor xml file. The zip would have the following structure.

	$ unzip -l ./tweetlib.zip 
	Archive:  ./tweetlib.zip
 	 Length     Date   Time    Name
 	--------    ----   ----    ----
  	 760817  04-23-14 17:16   hash-tags.jar
    	405  04-23-14 17:16   tweet.xml
	--------                   -------
   	761222                   2 files

Installing an AsterixDB Library We assume you have followed the ​instructions to set up a running AsterixDB instance. Let us refer your AsterixDB instance by the name "my_asterix".

**Step 1**: Stop the AsterixDB instance if it is in the ACTIVE state.

	$ managix stop  -n my_asterix

**Step 2**: Install the library using Managix install command. Just to illustrate, we use the help command to look up the syntax

	$ managix help  -cmd install
##Installs a library to an asterix instance.
Arguments/Options

	-n  Name of Asterix Instance
	-d  Name of the dataverse under which the library will be installed
	-l  Name of the library
	-p  Path to library zip bundle

Above is a sample output and explains the usage and the required parameters. Each library has a name and is installed under a dataverse. Recall that we had created a dataverse by the name - "feeds" prior to creating our datatypes and dataset. We shall name our library - "tweetlib", but ofcourse, you may choose another name.

You may download the pre-packaged library here​ and place the downloaded library (a zip bundle) at a convenient location on your disk. To install the library, use the Managix install command. An example is shown below.

	$ managix install -n my_asterix -d feeds -l tweetlib -p <put the absolute path of the library zip bundle here> 

You should see the following message:

	INFO: Installed library tweetlib

We shall next start our AsterixDB instance using the start command as shown below.

	$ managix start -n my_asterix

You may now use the AsterixDB library in AQL statements and queries. To look at the installed artifacts, you may execute the following query at the AsterixDB web-console.

	for $x in dataset Metadata.Function 
	return $x

	for $x in dataset Metadata.Library
	return $x

Our library is now installed and is ready to be used. So far we have done the following. 

1. Created a dataverse and defined the required datatypes 
2. Created a dataset to persist the ingested tweets
3. Created a Java UDF that would provide the pre-processing logic 
4. Packaged the Java UDF into an AsterixDB library and installed the library

The next logical step is to define the data feed.

##Creating a Data Feed

Our example data feed contains tweets that contain the keyword "Obama". We associate with the feed the pre-processing Java UDF that we created earlier. We use the built-in adaptor in AsterixDB - referred below by it's alias "pull_twitter". The adaptor uses the Twitter API to obtain the tweets that satisfies the condition, i.e. the tweet's text must contain the key word - "Obama".

	create feed TwitterFeed
	using "push_twitter"
	(("type-name"="Tweet"),("location"="US"));
	
	create secondary feed ProcessedTwitterFeed
	from feed TwitterFeed
	apply function "testlib#hashTags";


##Initiating the Data Flow

We shall next use the connect feed statement to initiate the flow of data.

	use dataverse feeds;
	connect feed TwitterFeed to dataset Tweets;
	connect feed ProcessedTwitterFeed to dataset ProcessedTweets;

The set of AQL statements required to create the datatypes, dataset, feed and initiating the dataflow can also be found here​.

##Querying the Ingested Data 
The following query provides the count of the ingested tweets:

	use dataverse feeds;
	count(for $x in dataset Tweets return $x);
	count(for $x in dataset ProcessedTweets return $x);

##Disconnect the feeds
The following query disconnects the feeds from the datasets:

	use dataverse feeds;
	disconnect feed TwitterFeed from dataset Tweets;
	disconnect feed ProcessedTwitterFeed from dataset ProcessedTweets;
