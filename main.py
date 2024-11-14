from indexer import *

i = Index()

#page = Page_Data('DEV/aiclub_ics_uci_edu/8ef6d99d9f9264fc84514cdd2e680d35843785310331e1db4bbd06dd2b8eda9b.json')
#tok_stream = page.get_tokens(10000)
i.crawl_json()