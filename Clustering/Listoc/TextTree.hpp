#ifndef TEXTTREE_HPP
#define TEXTTREE_HPP

#include <iostream>
#include <fstream>
#include <string>
#include <map>

using namespace std;
class TextTree {
public:
  TextTree();

  int add(string term, string key);
  int add(char * term, string key);
  void writeif(int tindex, string key);
  TextTree* search(char c, int tindex, string key);
  //static ofstream output;

private:
  string md5;
  long pindex;
  map<char, TextTree*> childs;
};

#endif
