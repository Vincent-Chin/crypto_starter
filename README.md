**CRYPTO STARTER KIT**

This is a starter kit that pulls data from cryptocompare over daily, hourly, and minutely timeframes.  There are also a
 couple of toy scripts evaluating several basic strategies.

**Getting Started**

1) This project is designed to be opened in PyCharm, using Python 3.6+.  Just install the packages listed in
requirements.txt.

2) Execute main() in demo.py.  This does some example grabs and dumps to the csv directory.  If you get an error, make
sure that there's a "csv" folder in your working directory prior to executing the code.  Note that there's also a
2000-bar limit on responses; if your date span is too large then you'll get an empty response instead.

3) Inspect get_data.py to see the meat of my API calls to cryptocompare.  Or just use the functions out of the box.

4) You can take a look at korean.py (run main()) - which is code I wrote up to investigate the so-called "kimchi"
premium.  Note it's just some thrust coding and therefore is an incomplete analysis.

5) You can also take a look at analyze.py (run main()).  I don't honestly fully remember what I was trying to do with
this code; I think I was trying to measure the autocorrelation of crypto-currencies and trying to estimate how likely
you are to win given an autocorrelation that's persisted at least n lags.

**Authors**

Vincent Chin

**License**

This project is licenses under the MIT License - see the LICENSE.md file for details.