import sys
import random
import logging
import json
import re

from preprocessing import Preprocessing
from annotation import annotate
from PySide6 import QtWidgets, QtGui, QtCore
from sql_formatter.core import format_sql

class GuiInterface(QtWidgets.QWidget):
  def __init__(self):
    super().__init__()
    self.setUI()
    self.annotate_index = {}
    self.reformatter_index = {

    }

  def setUI(self):
    self.setWindowTitle('Query')
    
    main_container = QtWidgets.QGridLayout()

    # DB connection container
    db_container = QtWidgets.QGroupBox()
    db_layout = QtWidgets.QGridLayout()

    self.db_lbl = QtWidgets.QLabel("Database:", self)
    db_layout.addWidget(self.db_lbl, 0, 0)
    self.db_tb = QtWidgets.QLineEdit(self)
    db_layout.addWidget(self.db_tb, 0, 1)

    self.user_lbl = QtWidgets.QLabel("Username:", self)
    db_layout.addWidget(self.user_lbl, 1, 0)
    self.user_tb = QtWidgets.QLineEdit(self)
    db_layout.addWidget(self.user_tb, 1, 1)

    self.pw_lbl = QtWidgets.QLabel("Password:", self)
    db_layout.addWidget(self.pw_lbl, 2, 0)
    self.pw_tb = QtWidgets.QLineEdit(self)
    self.pw_tb.setEchoMode(QtWidgets.QLineEdit.Password)
    db_layout.addWidget(self.pw_tb, 2, 1)

    self.connect_button = QtWidgets.QPushButton('Connect to database', self)
    self.connect_button.clicked.connect(self.onclick_connect)
    db_layout.addWidget(self.connect_button, 3, 0, 1, 2)

    db_container.setLayout(db_layout)

    # Query Container
    query_container = QtWidgets.QGroupBox()
    query_layout = QtWidgets.QGridLayout()

    self.query_lbl = QtWidgets.QLabel("Query: ", self)
    self.query_lbl.setStyleSheet("font-size: 16px;")
    query_layout.addWidget(self.query_lbl, 0, 0)

    self.query_ta = QtWidgets.QTextEdit(self)
    self.query_ta.setPlaceholderText("Enter query here.")
    query_layout.addWidget(self.query_ta, 1, 0)

    self.generate_button = QtWidgets.QPushButton('Generate', self)
    self.generate_button.clicked.connect(self.onclick_generate)
    query_layout.addWidget(self.generate_button, 2, 0)

    query_container.setLayout(query_layout)

    # Annotate Container
    annotate_container = QtWidgets.QGroupBox()
    annotate_layout = QtWidgets.QGridLayout()

    self.annotate_lbl = QtWidgets.QLabel("Annotated Query: ", self)
    self.annotate_lbl.setStyleSheet("font-size: 16px;")
    annotate_layout.addWidget(self.annotate_lbl, 0, 0)

    self.annotate_ta = QtWidgets.QListWidget()
    annotate_layout.addWidget(self.annotate_ta,1, 0)

    annotate_container.setLayout(annotate_layout)

    # Calling all containers
    main_container.addWidget(db_container, 0, 0, 1, 2)
    main_container.addWidget(query_container, 1, 0)
    main_container.addWidget(annotate_container, 1, 1)
    self.setLayout(main_container)

  # Button executable functions
  def onclick_connect(self):
    database = self.db_tb.text()
    username = self.user_tb.text()
    password = self.pw_tb.text()
    self.p = Preprocessing(database, username, password)
    
  def onclick_generate(self):
    if self.generate_button.text() == "Generate":
      # Disables text editor
      self.generate_button.setText("Edit")
      self.query_ta.setReadOnly(True)

      # Get text from text editor
      queryTxt = self.query_ta.toPlainText()

      # Formats text with query_Nl function
      formatted_queryTxt = self.query_Nl(queryTxt)
      self.formatted_doc = QtGui.QTextDocument(formatted_queryTxt)
      self.query_ta.setDocument(self.formatted_doc)
      

      #-------------FOR PROD-------------
      annotation = self.get_annotation(formatted_queryTxt)
      #-------------------------------------
      
      #-------------FOR TESTING-------------
      # annotation = [
      #   ('annotation_text_1fasdfasdvcasdca asdfascdsaca dascdsaferacdsac davcaefasdfca dacaesf asdf a dfadsfaw refasdfawr rafa dsf a', {'table':['supplier'],
      #                         'cond':["supplier.s_acctbal > 100000"]}),
      #   ('annotation_text_2', {'table':[],
      #                         'cond':['partsupp.ps_suppkey = supplier.s_suppkey', 'partsupp.ps_availqty > 1000']}),
      #   ('annotation_text_3', {'table':['partsupp'],
      #                         'cond':[]})
      # ]
      #-------------------------------------

      # Reads annotation list of tuples
      # Format could be annotation = 
      # [
      #   ('annotation_text_1', {'table':['supplier'],
      #                         'cond':['related_query_1', 'related_query_2']}),
      #   ('annotation_text_2', {'table':[],
      #                         'cond':['related_query_3']}),
      #   ('annotation_text_3', {'table':['partsupp'],
      #                         'cond':['partsupp.ps_suppkey = supplier.s_suppkey']})
      # ]
      self.buttons = QtWidgets.QButtonGroup(self)
      for (idx, (annotate_button, highlights)) in enumerate(annotation):

        # Populating annotate index
        self.annotate_index[idx] = highlights

        # Creating buttons then pushing them to QListWidget
        widget_item = QtWidgets.QListWidgetItem(self.annotate_ta)

        button = QtWidgets.QPushButton(f'{idx+1}. {annotate_button}', self)
        button.setStyleSheet("text-align: left; padding: 10px; word-break: break-all")
        
        
        self.buttons.addButton(button, idx)
        widget_item.setSizeHint(button.sizeHint())
        self.annotate_ta.addItem(widget_item)
        self.annotate_ta.setItemWidget(widget_item, button)

      self.buttons.idClicked.connect(self.show_highlights)
      
    else:
      #  Enables text editor while flushing all formatting
      self.flush_formatting()

      self.generate_button.setText("Generate")

      self.query_ta.setReadOnly(False)

      # Flushes annotate QListWidget
      self.annotate_ta.clear()
      


  # Helper functions
  def get_annotation(self, queryTxt):
      try:
          parsed_plan = self.p.getQEP(queryTxt)
          annotation = annotate(parsed_plan)
          return annotation
      except Exception as e:
          logging.error(e)
          self.p.conn.rollback()
          return 'Invalid query input'



  def query_Nl(self, queryTxt):
    """Formats query text by insertting new lines to increase readability.
    New lines are determined with keywords such as {select, where, from, group by, join, ...}
    https://www.w3schools.com/sql/sql_ref_keywords.asp
    https://pypi.org/project/sql-formatter/ 
    """
    formatted_query= format_sql(queryTxt)
    return formatted_query


  def show_highlights(self, idx):
    # print(f"{idx+1}. {self.annotate_index[idx]}")
    self.format_involved(self.annotate_index[idx]['table'],
                        self.annotate_index[idx]['cond'])

    self.highlighter.setDocument(self.query_ta.document())
    return
  
  def format_involved(self, tableList, conList):
    # set up pattern matching parameters
    self.highlighter = Highlighter()

    # flush old formatting
    self.flush_formatting()

    # tableList format
    tableList_format = QtGui.QTextCharFormat()
    tableList_format.setForeground(QtCore.Qt.blue)
    tableList_format.setFontWeight(QtGui.QFont.Bold)
    if (tableList):
      processed_tableString = '|'.join(tableList)
      pattern = r'^.*from.*(?:'+str(processed_tableString)+').*$'
      self.highlighter.add_mapping(pattern, tableList_format)


    # conList format
    conList_format = QtGui.QTextCharFormat()
    conList_format.setForeground(QtCore.Qt.red)
    conList_format.setFontItalic(True)
    if (conList):
      filtered_conList = self.filter_conList(conList)
      processed_conString = '|'.join(filtered_conList)
      pattern = r'(?:'+str(processed_conString)+')'
      self.highlighter.add_mapping(pattern, conList_format)

  def filter_conList(self, conList):
    filtered_conList = []

    for conditions in conList:
      conItems = conditions.split()
      if conItems[0].rfind('.'):
        conItems[0] = conItems[0][conItems[0].rfind('.')+1:]
        print(conItems[0])
      
      if conItems[2].rfind('.'):
        conItems[2] = conItems[2][conItems[2].rfind('.')+1:]
        print(conItems[2])

      perm1_conItem = '.*?'.join(conItems)
      perm2_conItem = '.*?'.join(reversed(conItems))
      filtered_conList.append(perm1_conItem)
      filtered_conList.append(perm2_conItem)

    return filtered_conList

  def flush_formatting(self):
    queryTxt = self.query_ta.toPlainText()
    queryDoc = QtGui.QTextDocument(queryTxt)
    self.query_ta.setDocument(queryDoc)
    return


class Highlighter(QtGui.QSyntaxHighlighter):
  
  def __init__(self, parent = None):
    super().__init__(parent)
    self.mapping = {}

  def add_mapping(self, pattern, pattern_format):
    self.mapping[pattern] = pattern_format

  def highlightBlock(self, text_block):
    for pattern, fmt in self.mapping.items():
      for match in re.finditer(pattern, text_block, re.IGNORECASE):
        start, end = match.span()
        self.setFormat(start, end-start, fmt)