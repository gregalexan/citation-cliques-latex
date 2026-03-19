MAIN = main
PDFLATEX = pdflatex
BIBER = biber

.PHONY: all clean distclean view

all: $(MAIN).pdf

$(MAIN).pdf: $(MAIN).tex references.bib
	$(PDFLATEX) $(MAIN)
	$(BIBER) $(MAIN)
	$(PDFLATEX) $(MAIN)
	$(PDFLATEX) $(MAIN)

view: $(MAIN).pdf
	xdg-open $(MAIN).pdf

clean:
	rm -f $(MAIN).aux $(MAIN).bbl $(MAIN).bcf $(MAIN).blg \
	      $(MAIN).log $(MAIN).out $(MAIN).run.xml $(MAIN).toc \
	      $(MAIN).lof $(MAIN).lot $(MAIN).fls $(MAIN).fdb_latexmk \
	      $(MAIN).nav $(MAIN).snm $(MAIN).vrb

distclean: clean
	rm -f $(MAIN).pdf
