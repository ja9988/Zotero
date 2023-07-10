Introducing Dodo!

What is Dodo?
Toto is Dodo's microservice stack.

Why did we have to make Toto? How will this affect the
user experience?
We had to make Toto because the current structure of
the scraping engine, GCPApps/friendchanges, is prone to
failure and isn't scalable. Additionally, in their past
form, our various applications weren't easily
maintainable. By splitting the application into many
microservices we can optimize every component
individually and different people/groups can manage the
function of the various microservices.
This won't affect the user the experience beyond 
possible irregularities in the timing of updates. In
general however, the updates should be more 
reliable/robust because there are fewer chances for
error within each unit of software.

What is the structure of Toto?
Toto is comprised of multiple microservices that each
have a small dedicated task. That means many of the
tasks done in GCPApps/friendchanges that can be done
in parallel are now microservices. So rather than
getting the friends metadata for each parent account 
serially or using joblib.parallel at the beginning of
the code, there is now a microservice called
toto/pagedispatch. toto/pagedispatch receives a message
from cloud scheduler or pub/sub containing user metadata
along with job information. toto/pagedispatch then sends 
messages to pub/sub for delivery to toto/getpage.

What do each of the microservices do?

* dodo/pagedispatch
-------------------
-> Receives a message from pub/sub or cloud scheduler.
If the message is from cloud scheduler or the pub/sub
message is missing metadata for a parent, a Twitter API
request is made to get the number of friends for each
parent username in the message. The metadata is then
used to calculate the pages that will have to be
requested and messages are sent via pub/sub containing
the username + metadata, page number, total pages,
and unique dispatch ID.

* dodo/getpage
--------------
-> Receives a message from pub/sub containing a username
and associated metadata, page number, total pages and a
unqiue dispatch ID. First the pub/sub message is acked
to prevent duplicate page scrapes. After the pub/sub
message is acked, a check is made against a table of 
completed page scrapes using the unique dispatch ID, 
page number and total pages. If this page has already
been succesfully scraped, then the page isn't scraped. 

* dodo/getmetadata
--------------
-> Receives a message from pub/sub containing a username
and associated metadata requests and a unqiue dispatch ID. 
First the pub/sub message is checked to prevent duplicate 
page scrapes. toto/pagedispatch will send a message to
pub/sub that will be forwarded to this service if grandparent
twitter metadata is malformed.
