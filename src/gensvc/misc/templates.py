templates = {}
templates['archive.sh'] = '''
#!/bin/bash -l
#SBATCH --job-name={__job_name}
#SBATCH --account=ISAAC-UTK0192
#SBATCH --partition=short
#SBATCH --qos=short
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=48
#SBATCH --time=0-03:00:00
#SBATCH --chdir {__utstor_dir}
#SBATCH --output=%x-%j.o
#SBATCH --mail-type=ALL
#SBATCH --mail-user=OIT_HPSC_Genomics@utk.edu

set -x
set -e
set -u
set -o pipefail

umask 002

declare rundir="{__rundir}"
declare runid="{__runid}"
declare utstor_dir="{__utstor_dir}"

declare tar_file="${{utstor_dir}}/${{runid}}.tar"
declare archive_complete="${{utstor_dir}}/${{runid}}.archivecomplete"

echo "START $(date) ($(date +%s))"

cd "$utstor_dir"

# If the target already exists, exit.
test -e "${{archive_complete}}" && exit 1
test -e "${{runid}}.inprogress" && exit 1
test -e "$runid" && exit 1

# "Make hardlinks to the data ..."
cp -lr "$rundir" "./${{runid}}.inprogress"
mv -n "./${{runid}}.inprogress" "./${{runid}}"

# "Calculate checksums ..."
cd "$runid"
find . -type f -not -name 'checksums.sha256*' -print0 | xargs -0 -L1 --max-procs 0 sha256sum > "checksums.sha256.inprogress"
mv -n "checksums.sha256.inprogress" "checksums.sha256"
cd -

# "Compress the data ..."
tar --exclude='*.fastq.gz' -cf "${{tar_file}}.inprogress" "${{runid}}"
mv -n "${{tar_file}}.inprogress" "${{tar_file}}"

# "Also checksum the tar file ..."
sha256sum "$(basename "${{tar_file}}")" > "${{tar_file}}.sha256.inprogress"
mv -n "${{tar_file}}.sha256.inprogress" "${{tar_file}}.sha256"

# "Cleaning up ..."
mv -n "$runid" "${{runid}}.archivecomplete"
# touch ArchiveSubmitComplete

echo "END $(date) ($(date +%s))"

exit 0
'''.strip()

# END
