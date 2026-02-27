import string
import os

from pyspark import SparkContext

sc = SparkContext("local[*]", "Sherlock Holmes Analysis")

file_path = "./sherlock_holmes.txt"

# O6: LOADING FROM MULTIPLE SOURCES *****
if not os.path.exists(file_path):
    print(f"Error: File '{file_path}' not found.")
    sc.stop()
    raise SystemExit(1)

entire_book = sc.textFile(file_path)
# O2 BLANK LINE COUNTER *****
blank_line_counter = sc.accumulator(0)
# Remove Header and Footer to keep only the actual story
# removing Project Gutenberg's information and licensing agreements
novel = entire_book.zipWithIndex()\
    .filter(lambda line: 58 <= line[1] < 12682)\
    .map(lambda line: line[0])

# accumulator counting
def count_blanks(line):
    global blank_line_counter
    if line.strip() == "":
        blank_line_counter.add(1)
    return line

# adds blanks lines to accumulator 
novel = novel.map(count_blanks)
print(f"Blank lines: {blank_line_counter.value}")

# TASK 1: TEXT NORMALIZATION =====
normalized = novel\
    .map(lambda line: line.lower()\
        .translate(str.maketrans('', '', string.punctuation)))
# print(normalized.take(10))


# TASK 2: WORD TOKENIZATION =====
tokenized = normalized\
    .flatMap(lambda line: line.split())

print(tokenized.take(20))

# TASK 3: FILTERING STOPWORDS =====
stopwords = {
    # Articles & Determiners
    'a', 'an', 'the', 'this', 'that', 'these', 'those', 'some', 'any', 'every',
    'each', 'both', 'few', 'more', 'most', 'other', 'such', 'only', 'all',
    'another', 'certain', 'either', 'neither', 'enough', 'several', 'various',
    
    # Pronouns
    'i', 'me', 'my', 'myself', 'mine',
    'you', 'your', 'yours', 'yourself', 'yourselves',
    'he', 'him', 'his', 'himself',
    'she', 'her', 'hers', 'herself',
    'it', 'its', 'itself',
    'we', 'us', 'our', 'ours', 'ourselves',
    'they', 'them', 'their', 'theirs', 'themselves',
    'who', 'whom', 'whose', 'which', 'what', 'whoever', 'whomever',
    'anyone', 'anybody', 'anything', 'someone', 'somebody', 'something',
    'everyone', 'everybody', 'everything', 'nobody', 'nothing', 'noone',
    'one', 'ones', 'self', 'selves',
    
    # Prepositions
    'in', 'on', 'of', 'at', 'as', 'by', 'with', 'from', 'to', 'for', 'about', 
    'into', 'between', 'through', 'during', 'before', 'after', 'above', 'below', 
    'under', 'over', 'up', 'down', 'against', 'out', 'off', 'across', 'along',
    'among', 'amongst', 'around', 'aside', 'behind', 'beside', 'besides', 'beyond',
    'concerning', 'despite', 'except', 'inside', 'outside', 'onto', 'per', 'toward',
    'towards', 'upon', 'via', 'within', 'without', 'throughout', 'underneath',
    'according', 'accordingly', 'near', 'past', 'regarding', 'regardless',
    
    # Conjunctions & Connectors
    'and', 'but', 'or', 'nor', 'not', 'no', 'so', 'yet', 'because', 'although', 
    'if', 'until', 'while', 'whereas', 'than', 'however', 'nevertheless', 
    'nonetheless', 'otherwise', 'moreover', 'furthermore', 'besides', 'therefore',
    'thus', 'hence', 'consequently', 'accordingly', 'inasmuch', 'insofar',
    'since', 'though', 'unless', 'whether', 'whereas', 'wherever', 'whenever',
    
    # Auxiliary Verbs & Contractions
    'am', 'is', 'are', 'was', 'were', 'be', 'being', 'been',
    'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'done',
    'can', 'could', 'will', 'would', 'shall', 'should', 'may', 'might', 'must',
    'ought', 'need', 'dare',
    "ain't", "aren't", "can't", "couldn't", "didn't", "doesn't", "don't", 
    "hadn't", "hasn't", "haven't", "isn't", "mightn't", "mustn't", "needn't",
    "shan't", "shouldn't", "wasn't", "weren't", "won't", "wouldn't",
    'ain', 'aren', 'cant', 'couldn', 'didn', 'doesn', 'don', 'hadn', 'hasn',
    'haven', 'isn', 'mightn', 'mustn', 'needn', 'shan', 'shouldn', 'wasn',
    'weren', 'won', 'wouldn', 'arent', 'cant', 'couldnt', 'didnt', 'doesnt',
    'dont', 'hadnt', 'hasnt', 'havent', 'isnt', 'mightnt', 'mustnt', 'neednt',
    'shant', 'shouldnt', 'wasnt', 'werent', 'wont', 'wouldnt',
    
    # Adverbs & Modifiers
    'very', 'too', 'just', 'own', 'same', 'how', 'why', 'again', 'further', 
    'then', 'once', 'here', 'there', 'when', 'where', 'now', 'ever', 'never',
    'always', 'often', 'sometimes', 'usually', 'already', 'still', 'also',
    'even', 'quite', 'rather', 'almost', 'nearly', 'hardly', 'barely', 'merely',
    'simply', 'certainly', 'definitely', 'probably', 'possibly', 'perhaps',
    'maybe', 'obviously', 'clearly', 'apparently', 'presumably', 'actually',
    'really', 'truly', 'exactly', 'entirely', 'completely', 'absolutely',
    'approximately', 'specifically', 'particularly', 'especially', 'significantly',
    'substantially', 'considerably', 'relatively', 'primarily', 'mainly', 'largely',
    'mostly', 'generally', 'normally', 'typically', 'usually', 'recently', 'lately',
    'soon', 'immediately', 'instantly', 'quickly', 'rapidly', 'slowly', 'gradually',
    'suddenly', 'briefly', 'shortly', 'directly', 'promptly', 'readily',
    'elsewhere', 'anywhere', 'everywhere', 'nowhere', 'somewhere', 'hereby',
    'herein', 'hereupon', 'hereafter', 'thereafter', 'thereby', 'therein',
    'thereupon', 'whereupon', 'wherein', 'whereby', 'whence', 'thence',
    'forward', 'forth', 'backwards', 'downwards', 'upwards', 'inward', 'outward',
    'apart', 'aside', 'away', 'back', 'home', 'together', 'alone',
    
    # Question Words
    'how', 'what', 'when', 'where', 'which', 'who', 'whom', 'whose', 'why',
    'whatever', 'whichever', 'whoever', 'whomever',
    
    # Demonstratives & Comparatives
    'this', 'that', 'these', 'those', 'such', 'same', 'similar', 'different',
    'better', 'worse', 'best', 'worst', 'less', 'least', 'more', 'most',
    
    # Quantifiers & Numbers
    'much', 'many', 'little', 'few', 'some', 'any', 'all', 'none', 'both',
    'either', 'neither', 'one', 'two', 'three', 'four', 'five', 'six', 'seven',
    'eight', 'nine', 'ten', 'eleven', 'twelve', 'fifteen', 'twenty', 'thirty',
    'forty', 'fifty', 'sixty', 'seventy', 'eighty', 'ninety', 'hundred', 'thousand',
    'million', 'first', 'second', 'third', 'fifth', 'zero',
    
    # Verbs (common action words)
    'allow', 'allows', 'appear', 'appreciate', 'appropriate', 'arise', 'ask',
    'asking', 'associated', 'become', 'becomes', 'becoming', 'began', 'begin',
    'beginning', 'beginnings', 'begins', 'believe', 'came', 'cause', 'causes',
    'come', 'comes', 'consider', 'considering', 'contain', 'containing', 'contains',
    'describe', 'described', 'detail', 'determine', 'develop', 'different', 'end',
    'ending', 'find', 'followed', 'following', 'follows', 'former', 'formerly',
    'found', 'gave', 'get', 'gets', 'getting', 'give', 'given', 'gives', 'giving',
    'go', 'goes', 'going', 'gone', 'got', 'gotten', 'happen', 'happens', 'help',
    'indicate', 'indicated', 'indicates', 'keep', 'keeps', 'kept', 'know', 'known',
    'knows', 'let', 'lets', 'like', 'liked', 'likely', 'look', 'looking', 'looks',
    'made', 'make', 'makes', 'making', 'mean', 'means', 'moved', 'move', 'obtain',
    'obtained', 'placed', 'please', 'provide', 'provides', 'put', 'ran', 'run',
    'said', 'saw', 'say', 'saying', 'says', 'seem', 'seemed', 'seeming', 'seems',
    'seen', 'sent', 'show', 'showed', 'shown', 'shows', 'showns', 'specify',
    'specifying', 'specified', 'stop', 'suggest', 'take', 'taken', 'taking',
    'tell', 'tends', 'thank', 'thanks', 'think', 'thought', 'tried', 'tries',
    'try', 'trying', 'took', 'use', 'used', 'useful', 'usefully', 'usefulness',
    'uses', 'using', 'value', 'want', 'wants', 'welcome', 'went', 'wish', 'wonder',
    'work', 'works', 'working',
    
    # Greetings & Polite Words
    'hello', 'hi', 'greetings', 'thanks', 'thank', 'thanx', 'please', 'sorry',
    'welcome', 'bye', 'goodbye',
    
    # Misc Common Words
    'yes', 'no', 'ok', 'okay', 'aye', 'nay', 'indeed', 'course', 'sure', 'zero',
    'call', 'fill', 'fire', 'fix', 'line', 'page', 'pages', 'part', 'sec',
    'section', 'side', 'top', 'bottom', 'front', 'inner', 'outer', 'tip',
    'date', 'day', 'year', 'time', 'example', 'way', 'world', 'case', 'kind',
    'name', 'number', 'order', 'place', 'point', 'thing', 'things', 'words',
    'bill', 'cry', 'importance', 'important', 'index', 'information', 'interest',
    'invention', 'miss', 'mug', 'mill', 'novel', 'research', 'result', 'resulted',
    'resulting', 'results', 'system', 'vol', 'vols',
    
    # Academic & Formal Terms
    'abst', 'accordingly', 'affected', 'affecting', 'affects', 'announce',
    'approximately', 'auth', 'available', 'briefly', 'cf', 'changes', 'cit',
    'corresponding', 'detail', 'effect', 'eg', 'et-al', 'etc', 'hither',
    'ibid', 'ie', 'immediate', 'importance', 'important', 'inc', 'indicated',
    'invention', 'ltd', 'mainly', 'meantime', 'meanwhile', 'necessarily',
    'necessary', 'noted', 'novel', 'omitted', 'par', 'pagecount', 'part',
    'particular', 'particularly', 'pas', 'poorly', 'possible', 'possibly',
    'potentially', 'predominantly', 'present', 'previously', 'primarily',
    'probably', 'promptly', 'proud', 'que', 'quickly', 'quite', 'qv',
    'readily', 'reasonably', 'recent', 'recently', 'ref', 'refs', 'related',
    'relatively', 'research-articl', 'respectively', 'sensible', 'serious',
    'seriously', 'significant', 'significantly', 'similarly', 'sincere',
    'slightly', 'specifically', 'strongly', 'sub', 'substantially', 'successfully',
    'sufficiently', 'sup', 'thoroughly', 'thou', 'thoughh', 'throug', 'thru',
    'til', 'unto', 'u201d', 'unfortunately', 'unlikely', 'ups', 'volumtype',
    'viz', 'widely', 'willing', 'www',
    
    # Single/Double Letters & Codes (academic references, etc)
    's', 't', 'a1', 'a2', 'a3', 'a4', 'ab', 'ac', 'ad', 'ae', 'af', 'ag', 'ah',
    'ai', 'aj', 'ak', 'al', 'am', 'ao', 'ap', 'aq', 'ar', 'au', 'av', 'aw', 'ax',
    'ay', 'az', 'b', 'b1', 'b2', 'b3', 'ba', 'bc', 'bd', 'bi', 'bj', 'bk', 'bl',
    'bn', 'bp', 'br', 'bs', 'bt', 'bu', 'bx', 'c', 'c1', 'c2', 'c3', 'ca', 'cc',
    'cd', 'ce', 'cg', 'ch', 'ci', 'cj', 'cl', 'cm', 'cn', 'co', 'com', 'con', 'cp',
    'cq', 'cr', 'cs', 'ct', 'cu', 'cv', 'cx', 'cy', 'cz', 'd', 'd2', 'da', 'dc',
    'dd', 'de', 'df', 'di', 'dj', 'dk', 'dl', 'dp', 'dr', 'ds', 'dt', 'du', 'dx',
    'dy', 'e', 'e2', 'e3', 'ea', 'ec', 'ed', 'edu', 'ee', 'ef', 'eg', 'ei', 'ej',
    'el', 'em', 'en', 'eo', 'ep', 'eq', 'er', 'es', 'est', 'et', 'eu', 'ev', 'ex',
    'ey', 'f', 'f2', 'fa', 'fc', 'ff', 'fi', 'fj', 'fl', 'fn', 'fo', 'fr', 'fs',
    'ft', 'fu', 'fy', 'g', 'ga', 'ge', 'gi', 'gj', 'gl', 'go', 'gr', 'gs', 'gy',
    'h', 'h2', 'h3', 'hh', 'hid', 'hj', 'ho', 'hr', 'hs', 'http', 'hu', 'hy', 'i2',
    'i3', 'i4', 'i6', 'i7', 'i8', 'ia', 'ib', 'ic', 'id', 'ie', 'ig', 'ih', 'ii',
    'ij', 'il', 'im', 'io', 'ip', 'iq', 'ir', 'iv', 'ix', 'iy', 'iz', 'j', 'jj',
    'jr', 'js', 'jt', 'ju', 'k', 'ke', 'kg', 'kj', 'km', 'ko', 'l', 'l2', 'la',
    'lb', 'lc', 'le', 'les', 'lest', 'lf', 'lj', 'll', 'ln', 'lo', 'los', 'lr',
    'ls', 'lt', 'm', 'm2', 'ma', 'mg', 'mi', 'ml', 'mn', 'mo', 'mr', 'mrs', 'ms',
    'mt', 'mu', 'n', 'n2', 'na', 'namely', 'nc', 'nd', 'ne', 'ng', 'ni', 'nj',
    'nl', 'nn', 'no', 'non', 'nos', 'nr', 'ns', 'nt', 'ny', 'o', 'oa', 'ob', 'oc',
    'od', 'og', 'oh', 'oi', 'oj', 'ok', 'ol', 'old', 'om', 'oo', 'op', 'oq', 'or',
    'ord', 'os', 'ot', 'ou', 'ow', 'owing', 'ox', 'oz', 'p', 'p1', 'p2', 'p3',
    'pc', 'pd', 'pe', 'pf', 'ph', 'pi', 'pj', 'pk', 'pl', 'plus', 'pm', 'pn',
    'po', 'pp', 'pq', 'pr', 'ps', 'pt', 'pu', 'py', 'q', 'qj', 'qu', 'r', 'r2',
    'ra', 'rc', 'rd', 're', 'rf', 'rh', 'ri', 'right', 'rj', 'rl', 'rm', 'rn',
    'ro', 'rq', 'rr', 'rs', 'rt', 'ru', 'rv', 'ry', 's2', 'sa', 'sc', 'sd', 'se',
    'sf', 'si', 'sj', 'sl', 'sm', 'sn', 'somethan', 'sp', 'sq', 'sr', 'ss', 'st',
    'sy', 'sz', 't1', 't2', 't3', 'tb', 'tc', 'td', 'te', 'tf', 'th', 'thats',
    'thickv', 'thin', 'ti', 'tj', 'tl', 'tm', 'tn', 'tp', 'tq', 'tr', 'ts', 'tt',
    'tv', 'tx', 'u', 'ue', 'ui', 'uj', 'uk', 'um', 'un', 'uo', 'ur', 'ut', 'v',
    'va', 'vd', 've', 'vj', 'vo', 'vq', 'vs', 'vt', 'vu', 'w', 'wa', 'we', 'wed',
    'well-b', 'whim', 'whither', 'wi', 'wo', 'wont', 'x', 'x1', 'x2', 'x3', 'xf',
    'xi', 'xj', 'xk', 'xl', 'xn', 'xo', 'xs', 'xt', 'xv', 'xx', 'y', 'y2', 'yj',
    'yl', 'yr', 'ys', 'yt', 'z', 'zi', 'zz', '0o', '0s', '3a', '3b', '3d', '6b',
    '6o', "a's", "c'mon", "c's", "he'd", "he'll", "he's", "here's", "how's",
    "i'd", "i'll", "i'm", "i've", "it'd", "it'll", "it's", "let's", "she'd",
    "she'll", "she's", "that'll", "that's", "that've", "there'll", "there's",
    "there've", "they'd", "they'll", "they're", "they've", "t's", "we'd",
    "we'll", "we're", "we've", "what'll", "what's", "when's", "where's",
    "who'd", "who'll", "who's", "why's", "you'd", "you'll", "you're", "you've",
    'amoungst', 'aint', 'cant', 'empty', 'far', 'full', 'hed', 'hes', 'howbeit',
    'ignored', 'itd', 'largely', 'nd', 'nt', 'thered', 'theres', 'therere', 'theyd',
    'theyre', 'whats', 'wheres', 'youd', 'youll', 'youre', 'whos', 'somethan'
}


# O1 Broadcast stopwords to worker nodes *****
broadcast_stopwords = sc.broadcast(stopwords)

stopwords_filtered = tokenized\
    .filter(lambda word: word not in broadcast_stopwords.value)

# O3 WORD-LENGTH PAIRING (PAIR RDD) *****
word_length_pairs = stopwords_filtered\
    .map(lambda word: (len(word), word))

print("Sample word-length pairs:")
for length, word in word_length_pairs.take(10):
    print(f"({length}, '{word}')")

print(stopwords_filtered.take(20))

# Task 4: CHARACTER COUNTING =====
char_frequency = entire_book\
    .flatMap(lambda line: list(line.lower()))\
    .filter(lambda char: char.isalpha())\
    .map(lambda char: (char, 1))\
    .reduceByKey(lambda a, b: a + b)\
    .sortBy(lambda x: x[1], ascending=False)

# total characters
total_chars = char_frequency\
    .map(lambda n: n[1])\
    .sum()
print(f"Total characters: {total_chars}")

# o4 CHARACTER FREQUENCY (PAIR RDD/REDUCEBYKEY) *****
print("Top 10 character frequencies:")
for char, count in char_frequency.take(10):
    print(f"('{char}', {count})")

# TASK 5: LINE LENGTH ANALYSIS =====
longest_line = novel\
    .map(lambda line: (line, len(line)))\
    .reduce(lambda a, b: a if a[1] > b[1] else b)

print(f"Longest line length: {longest_line[1]} characters")
print(f"Longest line content: {longest_line[0][:100]}...")  # Print first 100 chars

# TASK 6: WORD SEARCH (FILTERING) =====
watson_lines = normalized\
    .filter(lambda line: 'watson' in line)
print(f"Lines containing 'Watson': {watson_lines.count()}")
print("First 5 lines with 'Watson':")
for line in watson_lines.take(5):
    print(f"  {line[:80]}...")  # Print first 80 chars of each line

# TASK 7: UNIQUE VOCABULARY COUNT =====
def save_words_by_letter(rdd, output_dir):
    """Save unique words grouped by starting letter to folders"""
    words_by_letter = rdd\
        .filter(lambda word: word[0].isalpha())\
        .map(lambda word: (word[0], word))\
        .groupByKey()\
        .map(lambda x: (x[0], list(x[1])))
    
    for letter, words in words_by_letter.collect():
        sc.parallelize(words).coalesce(1).saveAsTextFile(f"{output_dir}/{letter}")
        print(f"Folder '{letter}' saved with {len(words)} unique words")


unique_words_rdd = stopwords_filtered.distinct()
unique_words = unique_words_rdd.count()
print(f"Unique vocabulary count: {unique_words} words")

# O5: Call function to save words by letter *****
save_words_by_letter(unique_words_rdd, "Holmes_Words_By_Letter")

# TASK 8: TOP 10 FREQUENT WORDS =====
def save_word_count_results(rdd, output_dir):
    """Save word count RDD to a directory"""
    rdd.coalesce(1).saveAsTextFile(output_dir)
    
word_count_rdd = stopwords_filtered\
    .filter(lambda word: word.isalpha())\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)

top_10_words = word_count_rdd\
    .sortBy(lambda x: x[1], ascending=False)\
    .take(10)

print("Top 10 frequent words")
for word, count in top_10_words:
    print(f"{word}: {count}")

# O7: save full word count results *****
save_word_count_results(word_count_rdd, "Holmes_WordCount_Results")


# TASK 9: SENTENCE START DISTRIBUTION =====
first_words = normalized\
    .map(lambda line: line.strip().split()[0].lower() if line.strip() else "")\
    .filter(lambda word: word != "")\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a, b: a + b)\
    .sortBy(lambda x: x[1], ascending=False)\
    .take(10)

print("Top 10 words starting in a sentence")
for word, count in first_words:
    print(f"{word}: {count}")

# TASK 10: AVERAGE WORD LENGTH =====
word_length = tokenized\
    .map(lambda word: (len(word), 1))\
    .reduce(lambda a, b: (a[0] + b[0], a[1] + b[1])) # sum + count

average_word_length = word_length[0] / word_length[1]
print(f"Average word length: {average_word_length}")

# TASK 11: DISTRIBUTION OF WORD LENGTHS =====
word_length_distribution = tokenized\
    .map(lambda word: (len(word), 1))\
    .reduceByKey(lambda a, b: a + b)\
    .sortByKey()\
    .collect()

print("Word length distribution:")
for length, count in word_length_distribution:
    print(f"{length}-letter words: {count}")

# TASK 12: SPECIFIC CHAPTER EXTRACTION =====
chapter_lines = novel\
    .zipWithIndex()\
    .filter(lambda x: "a scandal in bohemia" in x[0].lower())\
    .map(lambda x: x[1])\
    .collect()

end_lines = novel\
    .zipWithIndex()\
    .filter(lambda x: "the red-headed league" in x[0].lower())\
    .map(lambda x: x[1])\
    .collect()

if chapter_lines and end_lines:
    start_index = chapter_lines[0]
    end_index = end_lines[0]
    
    chapter_text = novel\
        .zipWithIndex()\
        .filter(lambda x: start_index <= x[1] < end_index)\
        .map(lambda x: x[0])\
        .collect()
    
    print(f"Chapter extracted from line {start_index} to {end_index}")
    print(f"Total lines in chapter: {len(chapter_text)}")
    print("\nFirst 5 lines of chapter:")
    for line in chapter_text[:5]:
        print(line)