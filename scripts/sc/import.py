import click
import os, sys, fnmatch
import re

'''
USAGE:
python import.py --idir=/Users/jspeiser/output/sc/2007/
'''

pattern = re.compile('(\w+).tsv(.bz2)*')
pattern1 = re.compile('(\w+)_(gender|color|loc|school_type|cid2).tsv(.bz2)*')

def parse_table(t):
    m = pattern1.search(t)
    if m:
        return "sc_" + m.group(1)
    m = pattern.search(t)
    if m:
        return "sc_" + m.group(1)

# via http://stackoverflow.com/questions/13299731/python-need-to-loop-through-directories-looking-for-txt-files
def findFiles (path, filter):
    for root, dirs, files in os.walk(path):
        for file in fnmatch.filter(files, filter):
            yield os.path.join(root, file)

@click.command()
@click.option('--idir', default='.', prompt=False,
              help='Directory for tsv files.')
def main(idir):
   for f in findFiles(idir, '*.tsv*'):
        print f, "Processing"
        if f.endswith("bz2"):
            os.system("bunzip2 -k " + f)
            f = f[:-4]
        print f
        handle = open(f)
        tablename = parse_table(f)
        print "table name =", tablename
        header = handle.readline().strip()
        fields = header.split('\t')
        print "fields", fields
        fields = [x for x in fields if x!='schools']
        fields[fields.index('class_id')] = 'classes'
        fields[fields.index('enroll_id')] = 'enrolled'
        fields[fields.index('school_id')] = 'num_schools'

        fields = ",".join(fields)
        print
        print

        cmd = '''mysql -h 127.0.0.1 -uroot $DATAVIVA_DB_NAME -e "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES (%s);" ''' % (f, tablename, fields)
        print cmd
        os.system(cmd)

if __name__ == '__main__':
    main()
