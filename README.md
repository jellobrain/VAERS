# VAERS
Use these two files to wrangle the VAERS datasets for the US up to 2021.

I know that the VAERS dataset comes with files that you can use to upload their data into a database, but I wanted to take a stab at doing it myself.

It is not perfect, but it works for the data up through 2021. In the same folder as your UNOPENED and UNZIPPED VAERS CSV files place the two files below.

When you get the .CVS file, DO NOT open them with a spreadsheet program, and the reason is that it will invariably change the formatting of your CSV in ways that will give you headaches.

Start with a database in your MySQL called VAERS. In the 'vaersload_aw.py' file, you will need to add your credentials. I have marked the spot with a TODO.

This 'vaersload_aw.py' file is the file you will run.  These are LARGE datasets, so be prepared and plan for this process to gobble up resources and time.  The good news is that once it is up, the individual queries you will no doubt invent to ask the data questions will take less time (but still some time) to complete.
