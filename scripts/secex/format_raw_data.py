import click
from os.path import basename

@click.command()
@click.argument('file_path', type=click.Path(exists=True))
@click.argument('secex_type')
@click.argument('output_path', default='output', type=click.Path())
def main(file_path, secex_type, output_path):
    start = time.time()

    df = pd.read_csv(file_path, sep=";")
    df['type'] = secex_type

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    new_file_path = os.path.abspath(os.path.join(output_path, "{0}.csv".format(basename(file_path))))
    df.to_csv(bz2.BZ2File(new_file_path, 'wb'), sep=";", index=True)
    
    total_run_time = (time.time() - start) / 60
    print "Done."; print;
    print "Total runtime: {0} minutes".format(int(total_run_time))
    print; print;

if __name__ == "__main__":
    main()