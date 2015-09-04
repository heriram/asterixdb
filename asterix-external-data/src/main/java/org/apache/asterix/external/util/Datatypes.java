/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.asterix.external.util;

public class Datatypes {

    /*public static final class Tweet {
        public static final String ID = "id";
        public static final String USER = "user";
        public static final String MESSAGE = "message_text";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";
        public static final String CREATED_AT = "created_at";
        public static final String SCREEN_NAME = "screen_name";
        public static final String COUNTRY = "country";
    }*/

    /*

    The following assumes this DDL (but ignoring the field name orders):

    create type TwitterUser if not exists as open{
        screen_name: string,
        language: string,
        friends_count: int32,
        status_count: int32,
        name: string,
        followers_count: string
    };

    create type Tweet if not exists as open{
        id: string,
        user: TwitterUser,
        latitude:double,
        longitude:double,
        created_at:string,
        message_text:string
    };

*/
    public static class Tweet {
        public static final String ID = "id";
        public static final String USER = "user";
        public static final String LATITUDE = "latitude";
        public static final String LONGITUDE = "longitude";
        public static final String CREATED_AT = "created_at";
        public static final String MESSAGE = "message_text";

        public static final String COUNTRY = "country";

        // User fields (for the sub record "user"
        public static final String SCREEN_NAME = "screen_name";
        public static final String LANGUAGE = "language";
        public static final String FRIENDS_COUNT = "friends_count";
        public static final String STATUS_COUNT = "status_count";
        public static final String NAME = "name";
        public static final String FOLLOWERS_COUNT = "followers_count";

    }


    /*

        create type ProcessedTweet if not exists as open {
            id: string,
            user_name:string,
            location:point,
            created_at:string,
            message_text:string,
            country: string,
            topics: [string]
        };
    */
    public static final class ProcessedTweet {
        public static final String USER_NAME = "user_name";
        public static final String LOCATION = "location";
        public static final String TOPICS = "topics";
    }


}
