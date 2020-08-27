for i in $(seq -f "%01g" 2014 2018)
do
    bamboo-cli --folder . --entry enh_100_pipeline --year="$i"
done
