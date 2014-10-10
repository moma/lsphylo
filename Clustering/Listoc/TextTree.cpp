#include "TextTree.hpp"

std::ofstream output;

TextTree::TextTree() {
  md5 = "";
  pindex = -1;
}

int TextTree::add(string term, string key) {
  return this->add(term.c_str(), key);
}

int TextTree::add(char * term, string key) {
  if(*term == 0) {
    md5 = key;
    return 0;
  }
  if(childs.find(term[0]) == childs.end())
    childs[term[0]] = new TextTree();
  return childs[term[0]]->add(term+1, key);
}

void TextTree::writeif(int tindex, string key) {
  if (md5 != "" && tindex > pindex) {
    output << key << "," << md5 << "\n";
    pindex = tindex;
  }
  return;
}

TextTree* TextTree::search(char c, int tindex, string key) {
  map<char, TextTree*>::iterator it;
  if ((it = childs.find(c)) == childs.end())
    return NULL;
  it->second->writeif(tindex, key);
  return it->second;
}

/*
ROOT := TextTree()
READ ALL TERM T
  ROOT.add(T, Tkey);
FOR EACH ARTICLE I:
  L = [ROOT]
  FOR EACH CARACTER C OF ARTICLE I:
    EACH NODE T IN L:
      T := T.search(C, I)
      IF T == NULL: L.erase(T)
    L.append(ROOT)
*/
