**First run**\
At first run of a script your data will be downloaded from OMDb and it will populate your data source, so it will take a while (app. 10 secs). When you run it next it won't download any data unless you explicitly tell it to do so by using command --add, so it will run much quicker.

Commands:  
  
**sort_by**  
  
use it like --sort_by column1, column2, column3 etc.  
You can sort by: title, year, runtime, genre, director, actors, writer, language, country, awards, rating, votes, boxoffice.
Choose any combination.
  
`python movies.py --sort_by runtime year boxoffice`  
  
**filter_by**  

use it like --filter_by category  
You can filter by: 
director, actor, eighty, oscar_nom, boxoffice, language  
Choose one category.  
  
`python movies.py --filter_by language spanish`  
  
**compare**  
  
use it like --compare category movie1 movie2  

You can compare them by:  
imdb, boxoffice, awards, runtime  
  
`python movies.py --compare imdb "Pulp Fiction" "The Godfather"`  
  
**add**  
  
use it like --add movie  
`python movies.py --add "The Dogfather"`  
  
**highscores**  
  
use it like --highscores  
  
`python movies.py --highscores`  
  
**api_key**\
Api key is provided, if you want to use it just copy it to where your movies.py file is. If you want use yours you need to create credentials file, in json format, name it credentials.json and make sure it contains a key 'apikey' with correct value of your apikey.
