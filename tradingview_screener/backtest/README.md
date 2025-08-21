backtesting is under review. It looks like backtest is too far from real trade

The problem is data index

Backtesting does not match real trades (entrance data does not match)




Colab talib issue; url = 'https://anaconda.org/conda-forge/libta-lib/0.4.0/download/linux-64/libta-lib-0.4.0-h166bdaf_1.tar.bz2'
!curl -L $url | tar xj -C /usr/lib/x86_64-linux-gnu/ lib --strip-components=1
!pip install conda-package-handling
!wget https://anaconda.org/conda-forge/ta-lib/0.5.1/download/linux-64/ta-lib-0.6.4-py312h196c9fc_0.conda
!cph x ta-lib-0.6.4-py312h196c9fc_0.conda
!mv ./ta-lib-0.6.4-py312h196c9fc_0/lib/python3.11/site-packages/talib /usr/local/lib/python3.11/dist-packages/ 


------------

failed with error: [Errno 2] No such file or directory: '/content/ta-lib-0.6.4-py312h196c9fc_0.conda'
mv: cannot stat './ta-lib-0.6.4-py312h196c9fc_0/lib/python3.11/site-packages/talib': No such file or directory
