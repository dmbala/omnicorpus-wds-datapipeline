module load python/3.12.11-fasrc02
mamba activate /n/holylfs06/LABS/kempner_shared/Everyone/common_envs/imgdl

python json_to_pair_wds_ip.py \
  --input-shard "/n/holylfs06/LABS/kempner_shared/Everyone/testbed/multimodal/OmniCorpus/webdataset_shards/CC-MAIN-2013-48/CC-MAIN-2013-48-000000.tar" \
  --output-shard "CC-MAIN-2013-48-%01d.tar" \
  --max-samples-per-shard 100 \
  --max-images-per-doc 20 

