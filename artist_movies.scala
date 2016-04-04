val artistsDF             = sqlContext.read.json("/home/hduser/artists_en.json");
val moviesDF              = sqlContext.read.json("/home/hduser/movies_en.json");
val moviesDFnoDesc        = moviesDF.select("actors","country","director","genre","id","title","year");
// As per year print the no of movies released in the year for all years
val no_of_movies_per_year = grpmovie_year.agg(count("title")).toDF("Year","No of Movies");

// Get the information on artists like first name and last name who worked on a movie
moviesDF.join(artistsDF,moviesDF("actors.id").cast(StringType).contains(artistsDF("id"))).select("first_name","last_name","title").show(200)







