# install docker
# https://serverfault.com/questions/836198/how-to-install-docker-on-aws-ec2-instance-with-ami-ce-ee-update

dt="2020-08-01"
tag="rakuten_app"
../bin/fetch_table_data.sh \
	./data/${dt} \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_user_behavior/tag=${tag}/dt=${dt}