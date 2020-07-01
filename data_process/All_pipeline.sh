# setup env
yum install -y tmux
curl https://bootstrap.pypa.io/get-pip.py | python3.6 - --user
pip3 install tqdm numpy scikit-learn gensim pandas annoy

# Downlaod data
# dt="2020-05-30"
dt="2020-05-30-2week"
./bin/fetch_table_data.sh \
	./data/${dt}/tr_data \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_train_raw/dt=${dt}

./bin/fetch_table_data.sh \
	./data/${dt}/te_data \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_test_raw/dt=${dt}

./bin/fetch_table_data.sh \
	./data/${dt}/w2v_tr_data \
	smartad-dmp/warehouse/user/seanchuang/i2i_offline_w2v_train_data/dt=${dt}

# Process Data
./data_process/0_raw_data_handler.py ${dt} data/${dt}
./data_process/test_session.py ${dt} data/${dt} --session_period=3600


# Run model



# Install spotlight
pip install git+https://github.com/maciejkula/spotlight.git@master#egg=spotlight