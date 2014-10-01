# Capture the current working directory
cwd=`pwd`

# Make .md2latex directory to work on generating final output.
mkdir -p $cwd/.md2latex/build

# Markdown file we are going to work on.
mdfile=$cwd/.md2latex/$1

# Latex output
texout=$cwd/.md2latex/pandocked.tex

# Final Latex
texfinal=$cwd/.md2latex/build/final.tex

# Build directory
build=$cwd/.md2latex/build

# Copy the markdown file to .md2latex directory
cp $1 $mdfile

# Run Pandoc to turn the markdown file with the bulk of the document into a .TeX file
pandoc -f markdown+grid_tables+simple_tables+pipe_tables --latex-engine=pdflatex -R -i $mdfile  -o $texout

# Remove some of the junk that Markdown adds when converting to TeX.
sed -i .bak 's/\[<+->\]//g' $texout
sed -i .bak 's/\\def\\labelenumi{\\arabic{enumi}.}//g' $texout
sed -i .bak 's/\\itemsep1pt\\parskip0pt\\parsep0pt//g' $texout

# Concatenate the header file (with the preambles, TOC, etc), the pandoc-created TeX file,
# and the footer file (with the bibliography) into a single buildable TeX file
cat $cwd/includes/header.tex $texout $cwd/includes/footer.tex > $texfinal

# Copy latex style files and bib files to build.
cp -r $cwd/*.cls $build
cp -r $cwd/*.bib $build

# Move into the build folder, which keeps the TeX junk and included files out of the main folder
cd $build

# Build PDF
latexmk final.tex -pdf -pdflatex="pdflatex -interaction=nonstopmode"

# Copy the PDF back
cp final.pdf $cwd

cd $cwd

# Open the PDF generated in my PDF reader of choice
if [ "$(uname)" == "Darwin" ]; then
    open final.pdf
elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]; then
    evince final.pdf
fi
