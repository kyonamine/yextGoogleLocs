# -*- coding: utf-8 -*-
"""
Created on Fri Aug 21 08:55:14 2015

@author: maddie
"""


import pymysql
import pymysql.cursors
import os
import sys


class ConnectToYextDB(object):

	def __init__(self,host=None,user='readonly',password=None,db='alpha',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor):
		
		self.host = host
		self.user = user
		self.password = password
		self.db = db
		self.charset = charset
		self.cursorclass = cursorclass
		self.connect_to_database()

	def connect_to_database(self):
		try:
			self.connection = pymysql.connect(host=self.host,
                             user=self.user,
                             password=self.password,
                             db=self.db,
                             charset=self.charset,
                             cursorclass=self.cursorclass)
		except Exception as e:
			print(e)
			print ("error connecting")
   
	def close_connection(self):
		try:
			self.connection.close()
		except:
			errorMessage = sys.exc_info()[0]
			print (errorMessage)


	def query_database(self,query):
		if query is None:
			raise ValueError("No query supplied.")
		elif not isinstance(query, str):
			raise ValueError("Query must be a string")
		try:
			self.cursor = self.connection.cursor()
			self.cursor.execute(query)
		except Exception as e:
			print("MYSQL ERROR: ")
			print(e)
			self.close_connection() #Force close sql connection to avoid clogging the system
		else:
				#return self.cursor.fetchall()
			row = self.cursor.fetchone()
			result = []
			while row is not None:
				result.append(row)
				row = self.cursor.fetchone()
			return result