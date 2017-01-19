import sys, os


GRASP_NAME = "schedule.grasp"

TASK_PROPERTIES = ["CompTime", "Period", "Deadline", "Priority", "Threshold"]

PROP = {}
for idx, par in enumerate(TASK_PROPERTIES):
	PROP[par] = idx



JOB_PROPERTIES = ["TimeRemaining", "Release", "Deadline", "Priority", "Threshold", "TaskID", "JobName"]
PARAM = {}
for idx, par in enumerate(JOB_PROPERTIES):
	PARAM[par] = idx

JOB_STATES = ["Arrive", "Resume", "Preempted", "Complete"]
STATE = {}
for idx,par in enumerate(JOB_STATES):
	STATE[par] = idx

class Scheduler():

	def __init__(self):

		self.__jobs = []
		self.__hyperPeriod = 0
		self.__preemptionPoints = []
		self.__schedule = []
		# Set currently executing job to -1
		self.__currJob = -1
		self.__graspHead = []
		self.__graspFile = open(GRASP_NAME, 'w')
		
	def WriteToGrasp(self):

		for line in self.__graspHead:
			line_temp = ""
			for item in line:
				line_temp = line_temp + str(item) + " "
			line_temp = line_temp + "\n"
			self.__graspFile.write(line_temp)

		for line in self.__schedule:
			line_temp = ""
			for item in line:
				line_temp = line_temp + str(item) + " "
			line_temp = line_temp + "\n"
			self.__graspFile.write(line_temp)

		self.__graspFile.close()


	def GetJsonDataFromApp(self):

		#Do the stuff for getting the data from JSON

		##

		# The final data must look like this
		#						Comp 	Period	Dead 	Prio 	Thres 					
		self.__taskJson = [ [	20, 	70, 	50, 	3, 		3	],
			  		  		[	20,		80,		80,		2,		3	],
			  		  		[	35,		200,	100,	1,		2 	]   
						  ]

		return self.__taskJson
		
	def CreateGraspHeader(self):

		for task in self.__taskJson:

			self.__graspHead.append([
									"newTask", 
									"task"+str(self.__taskJson.index(task)),
									"-priority "+str(task[PROP["Priority"]]),
									"-name "+"\"Task "+str(self.__taskJson.index(task))+"\""
									])

	def SplitTasksToJobs(self):

		if not self.__taskJson: #checks if the taskJson is not empty
			sys.exit("taskJson is empty")

		# pick a hyper-period
		self.__hyperPeriod = max(l[PROP['Period']] for l in self.__taskJson)

		taskSplitter = [ None  for i in range(len(self.__taskJson))]  #Temporary data type
		# Add release times for all tasks
		for task_id in range(len(self.__taskJson)):
			
			taskSplitter[task_id] = [0]

			temp_release = self.__taskJson[task_id][PROP['Period']]
			while(temp_release < self.__hyperPeriod):
				taskSplitter[task_id].append(temp_release)
				temp_release += temp_release

		for task in taskSplitter:
			task_idx = taskSplitter.index(task)
			counter  = 0
			for release in task:
				self.__jobs.append( [ self.__taskJson[task_idx][PROP['CompTime']],
									release, 
									release + self.__taskJson[task_idx][PROP['Deadline']] ,
									self.__taskJson[task_idx][PROP['Priority']],
									self.__taskJson[task_idx][PROP['Threshold']],
									task_idx,
									str(task_idx)+"."+str(counter)
								  ]
								)
				counter += 1
		return self.__jobs

	def FindPreemptionPoints(self):

		if not self.__jobs:
			sys.exit("Jobs not found. Split tasks to jobs first")

		for i in range(len(self.__jobs)):
			self.__preemptionPoints.append(self.__jobs[i][PARAM["Release"]]) #Here Period means release instant

		self.__preemptionPoints = sorted(list(set(self.__preemptionPoints)), key = int)

		return self.__preemptionPoints

	def FindArrivals(self, timestamp):
		
		self.__arrivals = []
		for job in self.__jobs:
			if job[PARAM["Release"]] == timestamp:
				self.__arrivals.append(self.__jobs.index(job))
		if not self.__arrivals:
			return None
		else:
			return self.__arrivals

	def FindActive(self, timestamp):

		self.__activeJobs = []

		for job in self.__jobs:
			if job[PARAM["Release"]] <= timestamp and job[PARAM["TimeRemaining"]] > 0:
				self.__activeJobs.append(self.__jobs.index(job))

		if not self.__activeJobs:
			return None
		else:
			return self.__activeJobs

	def FindHighestPriorityJob(self):
		# Finds the highest priority task among active jobs

		hp = -1
		hpJob = None
		
		for job_id in self.__activeJobs:
			if self.__jobs[job_id][PARAM["Priority"]] > hp:	
				hp = self.__jobs[job_id][PARAM["Priority"]]
				hpJob = job_id

		return hpJob 



	def Scheduler(self):

		for t in range(self.__hyperPeriod-1):

			# Update remaining time of currently executing task
			if self.__currJob != -1:
				self.__jobs[self.__currJob][PARAM["TimeRemaining"]] -= 1
			

			# Find Completed tasks (see if the current task is complete)
			
				if self.__jobs[self.__currJob][PARAM["TimeRemaining"]] == 0:
					self.__schedule.append([ "plot "+str(t), 
											"jobCompleted", 
											"job"+str(self.__jobs[self.__currJob][PARAM["JobName"]])
											])
					# Set currently executing job to none (-1)
					self.__currJob = -1

			# Find new arrivals if any
			self.FindArrivals(t)
			if self.__arrivals:
				for item in self.__arrivals:
					self.__schedule.append(["plot "+str(t),
											 "jobArrived", 
											 "job"+ str(self.__jobs[item][PARAM["JobName"]]), 
											 "task"+str(self.__jobs[item][PARAM["TaskID"]])
											 ])

			# Find already active tasks
			self.FindActive(t)

			## If no task is running, allocate processor to highest priority task among active tasks
			if self.__currJob == -1 and self.__activeJobs:
				hp = self.FindHighestPriorityJob()
				self.__currJob = hp
				
				self.__schedule.append([ "plot "+str(t),
										 "jobResumed",
										 "job"+str(self.__jobs[self.__currJob][PARAM["JobName"]])
										])


		print self.__schedule

if __name__ == '__main__':

	schedObj = Scheduler()
	schedObj.GetJsonDataFromApp()
	schedObj.CreateGraspHeader()
	schedObj.SplitTasksToJobs()
	schedObj.FindPreemptionPoints()
	schedObj.Scheduler()
	schedObj.WriteToGrasp()

	
