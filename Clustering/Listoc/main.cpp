
#include <iostream>
#include <fstream>
#include <vector>

#include "CSVTools.hpp"
#include "TextTree.hpp"

extern std::ofstream output;

int main(int argc, char** argv)
{
  std::ifstream terms, articles;
  TextTree *root;
  std::vector<TextTree*> clist;
  std::vector<TextTree*>::iterator it;
  long acount = 0;

  if(argc < 4) {
   std::cout << "Invalid arguments, usage: listoc <article> <terms> <outputname>\n";
   return 2;
  }

  terms.open(argv[2]);
  articles.open(argv[1]);
  root = new TextTree();
  output.open(argv[3]);

  for(CSVIterator loop(terms); loop != CSVIterator(); ++loop)
    root->add((*loop)[1], (*loop)[0]);

  std::cout << "Prefix tree constructed\n";
  
  for(CSVIterator loop(articles); loop != CSVIterator(); ++loop) {
    clist.clear();
    clist.push_back(root);
    for(int i = 0; i < (*loop)[1].length(); ++i) {
      for(it = clist.begin(); it != clist.end(); ) {
	if ( (*it = (*it)->search((*loop)[1][i], acount, (*loop)[0])) == NULL ) {
	  it = clist.erase(it);
	} else {
	  ++it;
	}
      }
      clist.push_back(root);
    }
    ++acount;
  }

  return 0;
}
