# postmodelling


## firing up the streamlit app

### optional: specify a dedicated port

```
ssh -f -i <path-to-ssh-private-key> -N -L localhost:<port>:localhost:<port> <username>@<server>
```

### running the streamlit app

```
streamlit run src/pipeline/postmodeling/postmodeling.py --server.address localhost --server.port 8501 --logger.level info --server.maxMessageSize 500 --server.runOnSave True
```

To run the streamlit app in debug mode, use command line parameter  `--logger.level debug`.