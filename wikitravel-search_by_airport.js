/*
Copyright (c) 2011 MondoWindow, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

*/

/*
Takes a TSV of airports and spits out a TSV of matching pages:

TSV input:

airportId	airportIATA	airportName	locationsServed

TSV output:

airportId	url	rank

to use: 

node.io -s -o wikitravel_search.tsv wikitravel-search_by_airport [skip_to_airport_id]

*/

var nodeio = require('node.io');

var escapeSQL = function(v) {
	if (v)
		return v.replace(/'/g, "''");
	else
		return "";
};

//amount of milliseconds to wait in between requests
var SLEEP_DELAY = 1000 * 30;

//amount of milliseconds to wait before auto-recovering after a timeout
var RETRY_DELAY = 1000 * 10;


//This is not the best function to use - things may be able to be restructured to use timeouts or
//the node.io "wait" option. However due to some weird bugs, this is the easiest solution for now.
var sleep = function(delay) {
	console.log("Sleeping " + (delay / 1000) + " seconds...");
	var later = new Date().getTime() + delay;
	while(new Date().getTime() < later) {
	   // do nothing
	}
};

//keeps track of where we left off.
var lastId = null;

//keeps track of auto-recovery
var skipTo = null;

var ignoredWordsRegex = /International|Airport|heliport|\(|\)|:/ig;

var methods = {
    input: 'airports.tsv',
    init: function() {
    	//AUTO-RECOVER
    	//pick up where we left off
		if (this.options.args[0]) {
			skipTo = parseInt(this.options.args[0]);
			console.log("Skipping to airport " + skipTo);
		}
    },
    run: function(row) {
		var values = this.parseValues(row, 'tsv');
   
   		if (values.length < 4 || !(parseInt(values[0]) > 0)) {
   			console.log("Skipping bad input: " + row);
   			this.skip();
   			return;
   		}
   
		//get a closure
		var nodeIO = this;	
		
		var airportId = values[0],
			airportIATA = values[1],
			airportName = values[2],
			airportCities = values[3].replace(ignoredWordsRegex, "").split(","); //replace(",", " ").split(" ");
		
		//AUTO-RECOVER
		//pick up where we left off
		if (skipTo > 0) {
			if (skipTo == airportId) { //we finally got to where we left off. stop skipping
				skipTo = 0;
			}
			else {
				this.skip();
				return;
			}
		}
		
		if (airportCities.length <= 0) {
   			console.log("Skipping bad input (no cities): " + row);
   			this.skip();
   			return;
   		}
		
		var url = "http://wikitravel.org/wiki/en/index.php?title=Special:Search&search={{IATA|" + airportIATA + "}}&fulltext=Search&offset=0&limit=50";
		
		//bulid regexes for matching title
		//first element is most important search term, descends from there.
		var airportCitiesRegex = [];
		var airportNames = airportName.replace(ignoredWordsRegex, "").split(" ");
		//airport name is pretty important, since wikipedia doesn't always list the actual city it serves
		if (airportNames[0].length > 0) {
			airportCitiesRegex.push(new RegExp(airportNames[0], "i"));
		}
		for (var i = 0; i < airportCities.length; i++ ) {
			if (airportCities[i].length > 0) {
				airportCitiesRegex.push(new RegExp(airportCities[i], "i"));
			}
		}
		//add the rest of the airport name for flavor
		for (var i = 1; i < airportNames.length; i++ ) {
			if (airportNames[i].length > 0) {
				airportCitiesRegex.push(new RegExp(airportNames[i], "i"));
			}
		}
		
		var parsePage = function(err, $) {
			//Handle any request / parsing errors
			if (err) {
				console.log('Error scraping page ' + url);
				console.log(err);

				//this implements auto-recover
				if (err.toString() == 'timeout'){ 
	                console.log("ERROR: Timeout. Auto-recovering in " + (RETRY_DELAY / 1000) + " seconds...");

					setTimeout(nodeIO.retry, RETRY_DELAY);
				} else {
					//nodeIO.exit(err);
					
					setTimeout(nodeIO.retry, RETRY_DELAY);
				}
				return;
			}

			var output = [];

			//output.push("/* PROCESSING PAGE " + url + " */");

			var rankedPages = [];

			$('#bodyContent ul li a').each(function (j, a) {
//console.log("Getting airport " + a.toString() + " - " + $(a).length);


				//find a match to the title
				var rank = 0;
				for (var i = 0; i < airportCitiesRegex.length; i++ ) {
					if (a.title.search(airportCitiesRegex[i]) >= 0) {
						//treat first city and airport name as most important (so we dont match generic countries like "France")
						if (i == 0) //airport
							rank += 500;
						else if (i == 1) //first city
							rank += 1000;
						else
							rank += airportCitiesRegex.length - i;
					}
				}
				if (rank > 0)
					rankedPages.push({"rank": rank, "title": a.title, "url": a.href});
					
				
			});
			
			//api blocks us if we go too fast!
			//this can be done by the "wait" option, but we can't because it also waits on skip() which we need for auto-recovery.
			sleep(SLEEP_DELAY);
			
			//save our spot if we suddenly crash
			lastId = airportId;
			
			if (rankedPages.length > 0) {
				//ORDER BY rank DESC
				rankedPages.sort(function (a, b) {
					return b.rank - a.rank;
				});
				
				for (var i = 0; i < rankedPages.length; i++ ) {
					output.push(airportId + "\t" + rankedPages[i]["url"] + "\t" + rankedPages[i]["rank"]);
				}
				
				//output.push("/*DONE*/");
				
				nodeIO.emit(output);
			}
			else {
				console.log("No matches found. Skipping " + row);
				nodeIO.skip();
				return;
			}	

		};
		
		
		this.getHtml(url, parsePage);		
    }
}

exports.job = new nodeio.Job({
	timeout:100, 
	jsdom:true,
	max: 1, 
	retry: 5 /*, can't use this because it also waits on skip() which we need to skip ahead to restored state.
	wait: SLEEP_DELAY / 1000 */
}, methods);