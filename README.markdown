Readme for disco-slct
=====================

A WARNING
---------
This is work in progress. Currently the code does not fully work as it is a
bit tricky to get Disco jobs to execute Disco jobs. Work is [currently on its
way to solve this
issue](http://groups.google.com/group/disco-dev/browse_thread/thread/e877d7c3017dbbbe).

What is this?
-------------

disco-slct is a mapreduce implementation of
[SLCT](http://ristov.users.sourceforge.net/slct/). According to the SLCT
website:

> SLCT is a tool that was designed to find clusters in logfile(s), so that
> each cluster corresponds to a certain line pattern that occurs frequently
> enough.

Examples of the clusters that SLCT, and thus disco-slct, is able to detect:

    Dec 18 * myhost.mydomain sshd[*]: log: Connection from * port *
    Dec 18 * myhost.mydomain sshd[*]: log: Password authentication for * accepted.

With the help of SLCT, one can quickly build a model of logfile(s), and also identify rare lines that do not fit the model (and are possibly anomalous).
disco-slct uses [Disco](http://discoproject.org/) for it's backend.

How to use it?
--------------

First, you obviously need to install [Disco](http://discoproject.org/).
Instructions on how to do this can be found in [the Disco installation
guide](http://discoproject.org/doc/start/install.html).

Optionally you'd want to push your logfiles to DDFS if you have a lot of them.
Information on how to do this can be found in [the Disco
tutorial](http://discoproject.org/doc/start/tutorial.html).

Next, you choose a threshold, which is the mininum support value for each log
pattern. This is the number of lines that each line outputted by disco-slct
will match in your log files.

Next, execute:

    $ python ./dslct.py -s <THRESHOLD> <DISCO_URL_TO_YOUR_LOGFILE>

You can always issue

    $ python ./dslct.py --help

to see exact parameters to disco-slct.
