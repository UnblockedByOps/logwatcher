class echo:
	def __init__(self, debug=False, config={}):
		self.lines_processed = 0
	
	def process_line(self, line):
		if line[0] == "2":
			raise Exception("Oh no, a 2!")
		self.lines_processed += 1
		print line
		return line

	def get_metrics(self):
		ret = {"lines_processed": self.lines_processed}
		self.lines_processed = 0
		return ret

