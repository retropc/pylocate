#!/usr/bin/env python
import gtk, os, sys, trie, platform, ctypes, threading, time

if platform.system() == "Windows":
  exc = lambda x: ctypes.windll.shell32.ShellExecuteW(0, u'open',  ctypes.c_wchar_p(x), None, None, 1)
else:
  exc = lambda x: os.spawnvp(os.P_NOWAIT, OPENER, ["xdg-open", x])

SCRIPTPATH, _ = os.path.split(sys.argv[0])

class IndexThread(threading.Thread):
  def __init__(self, index, max=500):
    threading.Thread.__init__(self)
    self.setDaemon(True)
    self.__index = index
    self.__terminated = False
    self.__stopped = False
    self.__cv = threading.Condition()
    self.__data = None
    self.__max = max
    
  def wakeup(self, data):
    self.__cv.acquire()
    try:
      self.__data = data
      self.__stopped = True
      self.__cv.notify()
    finally:
      self.__cv.release()
    
  def run(self):
    while not self.__terminated:
      self.__cv.acquire()
      try:
        while self.__data == None and not self.__terminated:
          self.__cv.wait()
          
        if self.__terminated:
          return
          
        self.__stopped = False
        data = self.__data
        self.__data = None
      finally:
        self.__cv.release()
        
      self.work(data)
      
  def terminate(self):
    self.__cv.acquire()
    try:
      self.__addfn = lambda x: 0
      self.__terminated = True    
      self.__stopped = True
      self.__cv.notify()
    finally:
      self.__cv.release()
    
  def stopped(self):
    self.__cv.acquire()
    try:
      return self.__stopped
    finally:
      self.__cv.release()
      
  def work(self, data):
    if data == "":
      return

    t = time.time()
    l = []
    first = True
    
    for i, x in enumerate(self.__index[data]):
      if self.stopped():
        return
      if i > self.__max:
        break
        
      l.append(x)
      nt = time.time()
      # only add stuff if there's been an noticeable time difference
      if nt - t > 0.1 or first and len(l) > 10:
        t = nt
        self.addfn(l, first)
        first = False
        l = []
        
    self.addfn(l, first)
    
class PyIndexGUI:
  def __init__(self, index, indexthread):
    self.__index = index
    self.__indexthread = indexthread
    self.__indexthread.addfn = self.add
    self.__data = []

    builder = gtk.Builder()
    builder.add_from_file(os.path.join(SCRIPTPATH, "gui.xml"))

    builder.connect_signals(self)

    self.__window = builder.get_object("window")
    self.__treeview = builder.get_object("results")
    self.__statusbar = builder.get_object("statusbar")
    
    self.__setup_treeview()

  def __setup_treeview(self):
    t = self.__treeview
    self.__listmodel = gtk.ListStore(str, str)
    t.set_model(self.__listmodel)

    cell = gtk.CellRendererText()
    col = gtk.TreeViewColumn("Filename")
    col.pack_start(cell, True)
    col.set_attributes(cell, text=0)
    t.append_column(col)

#    col = gtk.TreeViewColumn("Directory")
#    cell = gtk.CellRendererText()
#    col.pack_start(cell)
#    col.set_attributes(cell, text=1)
#    t.append_column(col)

  def show(self):
    self.__window.show()

  def on_window_destroy(self, widget, data=None):
    gtk.main_quit()

  def on_window_key_press_event(self, widget, data=None):
    if data.keyval == gtk.keysyms.Escape:
      self.__window.destroy()

  def get_full_path(self, column):
    return os.path.join(self.__index.metadata["base"], self.__data[column[0]])

  def on_results_row_activated(self, widget, column, data=None):
    exc(self.get_full_path(column))
    self.__window.destroy()

  def on_results_button_press_event(self, widget, event):
    if event.type != gtk.gdk.BUTTON_PRESS or event.button not in (2, 3):
      return
    
    tree = self.__treeview.get_path_at_pos(int(event.x), int(event.y))
    if not tree:
      return
      
    p = self.get_full_path(tree[0])
    if event.button == 3:
      p = os.path.dirname(p)
    exc(p)

  def set_statusbar(self, text):
    cid = self.__statusbar.get_context_id("Status bar")
    self.__statusbar.pop(cid)
    self.__statusbar.push(cid, text)

  def clear(self):
    self.__data = []
    self.__listmodel.clear()
    self.set_statusbar("")
    
  def on_results_cursor_changed(self, widget, data=None):
    row = self.__treeview.get_selection().get_selected_rows()[1][0]
    self.set_statusbar(self.get_full_path(row))
    
  def on_search_changed(self, widget, data=None):
    t = widget.get_text()

    self.__indexthread.wakeup(t)
    if t == "":
      self.clear()
    
  def add(self, data, first):
    gtk.gdk.threads_enter()
    try:
      if first:
        self.clear()
      
      for x in data:
        self.__data.append(os.path.join(*x))
        self.__listmodel.append(x[::-1])
      self.__treeview.columns_autosize()
    finally:
      gtk.gdk.threads_leave()
    
def alert(text):
  q = gtk.MessageDialog(None, gtk.DIALOG_DESTROY_WITH_PARENT, gtk.MESSAGE_INFO, gtk.BUTTONS_OK, text.encode("utf-8"))
  q.run()
  q.destroy()

def main(indexfn):
  i = trie.FIndexReadTrie(indexfn)
  try:
    it = IndexThread(i)
    it.start()
    try:
      window = PyIndexGUI(i, it)
      window.show()
      gtk.gdk.threads_init()
      gtk.main()
    finally:
      it.terminate()
      it.join()
  finally:
    i.close()
    
if __name__ == "__main__":
  if len(sys.argv) < 2:
    alert("usage: %s [index file]" % sys.argv[0])
  else:
    main(sys.argv[1])
