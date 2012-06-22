package ca.uwaterloo.twitter;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.set.OpenCharHashSet;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Queues;

public class TokenIterator extends AbstractIterator<String> {
  
  // This is a bad idea since the latin languages cannot be disambiguated
  // public static class ASCIITokenIterator extends TokenIterator {
  //
  // public ASCIITokenIterator(String string) {
  // super(string);
  // }
  //
  // public ASCIITokenIterator(Text txt) {
  // super(txt);
  // }
  //
  // @Override
  // protected boolean isTokenChar(int c) {
  //
  // return super.isTokenChar(c) && c < 128;
  // }
  // }
  
  public static class LatinTokenIterator extends TokenIterator {
    
    public LatinTokenIterator(String string) {
      super(string);
    }
    
    public LatinTokenIterator(Text txt) {
      super(txt);
    }
    
    @Override
    protected boolean isTokenChar(int c) {
      // Latin and Latin extended are all below '\u024F'
      return super.isTokenChar(c) && c <= (int) '\u024F';
    }
  }
  
  public static final String URL_PLACEHOLDER = "URL";
  
  public static final String PARAM_REPEAT_HASHTAG = "repHT";
  
  // TODONE: is it really better to work with ints?? or is char[] good? Yes.. so that you can have
  // -1 place holders
  private final char[] chs;
  private int cIx = 0;
  private LinkedBlockingDeque<String> pendingRes = Queues
      .<String> newLinkedBlockingDeque();
  private static int[] BLANK_3CH = { -1, -1, -1 };
  private int[] repeatedChs = Arrays.copyOf(BLANK_3CH, 3);
  private OpenCharHashSet punctuation;
  private boolean repeatHashTag = false;
  
  public TokenIterator(Text input) {
    // TODONOT: create a map reduce step for language categorization first
    // TextCategorizer langCat = new TextCategorizer();
    // if(!"english".equals(langCat.categorize(txt.toString()))){
    // L.warn("Prunning non-english pattern: {}", txt);
    // continue;
    // }
    
    this.chs = input.toString().toCharArray();
    //  removed _ because it can be used in usernames..  
    String punctuationStr = "!$%&()*+,./:;<=>?[\\]^{|}~\"'`-"; 
    punctuation = new OpenCharHashSet(punctuationStr.length(), 0, 0.999);
    for (char ch : punctuationStr.toCharArray()) {
      punctuation.add(ch);
    }
  }
  
  public TokenIterator(String string) {
    this(new Text(string));
  }
  
  public TokenIterator setRepeatHashTag(boolean b) {
    repeatHashTag = b;
    return this;
  }
  
  public boolean getRepeatHashTag() {
    return repeatHashTag;
  }
  
  @Override
  protected String computeNext() {
    if (!pendingRes.isEmpty()) {
      return pendingRes.removeFirst();
    }
    
    if (cIx > chs.length - 1) {
      return endOfData();
    }
    int ch = -1;
    StringBuilder result = new StringBuilder();
    while (cIx < chs.length) {
      ch = chs[cIx++];
      if (isTokenChar(ch)) {
        int tch = normalize(ch);
        
        // normalize repetitions to 3 chars (cooooooooooooooooooool --> coool)
        if ((repeatedChs[0] == repeatedChs[1])
            && (repeatedChs[1] == repeatedChs[2])
            && (repeatedChs[2] == tch)
            && !Character.isDigit(tch)) {
          continue;
        } else {
          repeatedChs[0] = repeatedChs[1];
          repeatedChs[1] = repeatedChs[2];
          repeatedChs[2] = tch;
        }
        
        result.append((char) tch);
      } else {
        
        // reset repetition detector
        repeatedChs = Arrays.copyOf(BLANK_3CH, 3);
        
        if (isDelimiter(ch)) {
          break;
        }
      }
    }
    
    if (result.length() == 0) { // < 3) {// || isStopWord(res)) {
      return computeNext();
    }
    
    if (repeatHashTag && result.charAt(0) == '#' && result.length() > 1) { // || result.charAt(0) ==
                                                                           // '@') {
      pendingRes.addLast(result.substring(1));
    }
    
    String ret = result.toString();
    if ((ret.startsWith("http") && ch==':') || (ret.startsWith("www") && ch =='.')) {
      ret = URL_PLACEHOLDER;
      while (cIx < chs.length) {
        if (!Character.isWhitespace(ch)) {
          ch = chs[cIx++];
        } else {
          break;
        }
      }
    } else if (ret.matches("\\d+\\z")) {
      // A number, do not delimit on commas and dots,
      // but delimit on dashes and slashes (dates)
      // cIx is already ahead of the character that caused
      // delimiting of the token
      --cIx;
      while (cIx < chs.length) {
        ch = chs[cIx++];
        if (isTokenChar(ch) || isDecimalSeparator(ch)) {
          result.append((char) ch);
        } else {
          if(isThousandsSeparator(ch)){
            continue;
          }
          // no need to check for being delimiter, because
          // we should proceed only if we are in a number token
          ret = result.toString();
          break;
        }
      }
    }
    
    return ret;
  }
  
  protected boolean isThousandsSeparator(int ch) {
    return ch == ',';
  }
  
  protected boolean isDecimalSeparator(int ch) {
    return ch == '.';
  }
  
  protected boolean isDelimiter(int c) {
    return Character.isWhitespace(c) || punctuation.contains((char) c);
  }
  
  protected boolean isTokenChar(int c) {
    return Character.isLetter(c) || Character.isDigit(c) || c == (int) '#'
        || c == (int) '@' || c == (int)'_';
  }
  
  protected int normalize(int c) {
    return Character.isLetter(c) ? Character.toLowerCase(c) : c;
  }
  
  // protected boolean isStopWord(String token){
  // //do we need synchronization or multiple copies?
  // return stopWords.contains(token);
  // }
  //
  // static HashSet<String> stopWords = Sets.newHashSet(
  // "a",
  // "a's",
  // "able",
  // "about",
  // "above",
  // "according",
  // "accordingly",
  // "across",
  // "actually",
  // "after",
  // "afterwards",
  // "again",
  // "against",
  // "ain't",
  // "all",
  // "allow",
  // "allows",
  // "almost",
  // "alone",
  // "along",
  // "already",
  // "also",
  // "although",
  // "always",
  // "am",
  // "among",
  // "amongst",
  // "an",
  // "and",
  // "another",
  // "any",
  // "anybody",
  // "anyhow",
  // "anyone",
  // "anything",
  // "anyway",
  // "anyways",
  // "anywhere",
  // "apart",
  // "appear",
  // "appreciate",
  // "appropriate",
  // "are",
  // "aren't",
  // "around",
  // "as",
  // "aside",
  // "ask",
  // "asking",
  // "associated",
  // "at",
  // "available",
  // "away",
  // "awfully",
  // "b",
  // "be",
  // "became",
  // "because",
  // "become",
  // "becomes",
  // "becoming",
  // "been",
  // "before",
  // "beforehand",
  // "behind",
  // "being",
  // "believe",
  // "below",
  // "beside",
  // "besides",
  // "best",
  // "better",
  // "between",
  // "beyond",
  // "both",
  // "brief",
  // "but",
  // "by",
  // "c",
  // "c'mon",
  // "c's",
  // "came",
  // "can",
  // "can't",
  // "cannot",
  // "cant",
  // "cause",
  // "causes",
  // "certain",
  // "certainly",
  // "changes",
  // "clearly",
  // "co",
  // "com",
  // "come",
  // "comes",
  // "concerning",
  // "consequently",
  // "consider",
  // "considering",
  // "contain",
  // "containing",
  // "contains",
  // "corresponding",
  // "could",
  // "couldn't",
  // "course",
  // "currently",
  // "d",
  // "definitely",
  // "described",
  // "despite",
  // "did",
  // "didn't",
  // "different",
  // "do",
  // "does",
  // "doesn't",
  // "doing",
  // "don't",
  // "done",
  // "down",
  // "downwards",
  // "during",
  // "e",
  // "each",
  // "edu",
  // "eg",
  // "eight",
  // "either",
  // "else",
  // "elsewhere",
  // "enough",
  // "entirely",
  // "especially",
  // "et",
  // "etc",
  // "even",
  // "ever",
  // "every",
  // "everybody",
  // "everyone",
  // "everything",
  // "everywhere",
  // "ex",
  // "exactly",
  // "example",
  // "except",
  // "f",
  // "far",
  // "few",
  // "fifth",
  // "first",
  // "five",
  // "followed",
  // "following",
  // "follows",
  // "for",
  // "former",
  // "formerly",
  // "forth",
  // "four",
  // "from",
  // "further",
  // "furthermore",
  // "g",
  // "get",
  // "gets",
  // "getting",
  // "given",
  // "gives",
  // "go",
  // "goes",
  // "going",
  // "gone",
  // "got",
  // "gotten",
  // "greetings",
  // "h",
  // "had",
  // "hadn't",
  // "happens",
  // "hardly",
  // "has",
  // "hasn't",
  // "have",
  // "haven't",
  // "having",
  // "he",
  // "he's",
  // "hello",
  // "help",
  // "hence",
  // "her",
  // "here",
  // "here's",
  // "hereafter",
  // "hereby",
  // "herein",
  // "hereupon",
  // "hers",
  // "herself",
  // "hi",
  // "him",
  // "himself",
  // "his",
  // "hither",
  // "hopefully",
  // "how",
  // "howbeit",
  // "however",
  // "i",
  // "i'd",
  // "i'll",
  // "i'm",
  // "i've",
  // "ie",
  // "if",
  // "ignored",
  // "immediate",
  // "in",
  // "inasmuch",
  // "inc",
  // "indeed",
  // "indicate",
  // "indicated",
  // "indicates",
  // "inner",
  // "insofar",
  // "instead",
  // "into",
  // "inward",
  // "is",
  // "isn't",
  // "it",
  // "it'd",
  // "it'll",
  // "it's",
  // "its",
  // "itself",
  // "j",
  // "just",
  // "k",
  // "keep",
  // "keeps",
  // "kept",
  // "know",
  // "knows",
  // "known",
  // "l",
  // "last",
  // "lately",
  // "later",
  // "latter",
  // "latterly",
  // "least",
  // "less",
  // "lest",
  // "let",
  // "let's",
  // "like",
  // "liked",
  // "likely",
  // "little",
  // "look",
  // "looking",
  // "looks",
  // "ltd",
  // "m",
  // "mainly",
  // "many",
  // "may",
  // "maybe",
  // "me",
  // "mean",
  // "meanwhile",
  // "merely",
  // "might",
  // "more",
  // "moreover",
  // "most",
  // "mostly",
  // "much",
  // "must",
  // "my",
  // "myself",
  // "n",
  // "name",
  // "namely",
  // "nd",
  // "near",
  // "nearly",
  // "necessary",
  // "need",
  // "needs",
  // "neither",
  // "never",
  // "nevertheless",
  // "new",
  // "next",
  // "nine",
  // "no",
  // "nobody",
  // "non",
  // "none",
  // "noone",
  // "nor",
  // "normally",
  // "not",
  // "nothing",
  // "novel",
  // "now",
  // "nowhere",
  // "o",
  // "obviously",
  // "of",
  // "off",
  // "often",
  // "oh",
  // "ok",
  // "okay",
  // "old",
  // "on",
  // "once",
  // "one",
  // "ones",
  // "only",
  // "onto",
  // "or",
  // "other",
  // "others",
  // "otherwise",
  // "ought",
  // "our",
  // "ours",
  // "ourselves",
  // "out",
  // "outside",
  // "over",
  // "overall",
  // "own",
  // "p",
  // "particular",
  // "particularly",
  // "per",
  // "perhaps",
  // "placed",
  // "please",
  // "plus",
  // "possible",// protected boolean isStopWord(String token){
  // //TODO: do we need synchronization or multiple copies?
  // return stopWords.contains(token);
  // }
  //
  // static HashSet<String> stopWords = Sets.newHashSet(
  // "a",
  // "a's",
  // "able",
  // "about",
  // "above",
  // "according",
  // "accordingly",
  // "across",
  // "actually",
  // "after",
  // "afterwards",
  // "again",
  // "against",
  // "ain't",
  // "all",
  // "allow",
  // "allows",
  // "almost",
  // "alone",
  // "along",
  // "already",
  // "also",
  // "although",
  // "always",
  // "am",
  // "among",
  // "amongst",
  // "an",
  // "and",
  // "another",
  // "any",
  // "anybody",
  // "anyhow",
  // "anyone",
  // "anything",
  // "anyway",
  // "anyways",
  // "anywhere",
  // "apart",
  // "appear",
  // "appreciate",
  // "appropriate",
  // "are",
  // "aren't",
  // "around",
  // "as",
  // "aside",
  // "ask",
  // "asking",
  // "associated",
  // "at",
  // "available",
  // "away",
  // "awfully",
  // "b",
  // "be",
  // "became",
  // "because",
  // "become",
  // "becomes",
  // "becoming",
  // "been",
  // "before",
  // "beforehand",
  // "behind",
  // "being",
  // "believe",
  // "below",
  // "beside",
  // "besides",
  // "best",
  // "better",
  // "between",
  // "beyond",
  // "both",
  // "brief",
  // "but",
  // "by",
  // "c",
  // "c'mon",
  // "c's",
  // "came",
  // "can",
  // "can't",
  // "cannot",
  // "cant",
  // "cause",
  // "causes",
  // "certain",
  // "certainly",
  // "changes",
  // "clearly",
  // "co",
  // "com",
  // "come",
  // "comes",
  // "concerning",
  // "consequently",
  // "consider",
  // "considering",
  // "contain",
  // "containing",
  // "contains",
  // "corresponding",
  // "could",
  // "couldn't",
  // "course",
  // "currently",
  // "d",
  // "definitely",
  // "described",
  // "despite",
  // "did",
  // "didn't",
  // "different",
  // "do",
  // "does",
  // "doesn't",
  // "doing",
  // "don't",
  // "done",
  // "down",
  // "downwards",
  // "during",
  // "e",
  // "each",
  // "edu",
  // "eg",
  // "eight",
  // "either",
  // "else",
  // "elsewhere",
  // "enough",
  // "entirely",
  // "especially",
  // "et",
  // "etc",
  // "even",
  // "ever",
  // "every",
  // "everybody",
  // "everyone",
  // "everything",
  // "everywhere",
  // "ex",
  // "exactly",
  // "example",
  // "except",
  // "f",
  // "far",
  // "few",
  // "fifth",
  // "first",
  // "five",
  // "followed",
  // "following",
  // "follows",
  // "for",
  // "former",
  // "formerly",
  // "forth",
  // "four",
  // "from",
  // "further",
  // "furthermore",
  // "g",
  // "get",
  // "gets",
  // "getting",
  // "given",
  // "gives",
  // "go",
  // "goes",
  // "going",
  // "gone",
  // "got",
  // "gotten",
  // "greetings",
  // "h",
  // "had",
  // "hadn't",
  // "happens",
  // "hardly",
  // "has",
  // "hasn't",
  // "have",
  // "haven't",
  // "having",
  // "he",
  // "he's",
  // "hello",
  // "help",
  // "hence",
  // "her",
  // "here",
  // "here's",
  // "hereafter",
  // "hereby",
  // "herein",
  // "hereupon",
  // "hers",
  // "herself",
  // "hi",
  // "him",
  // "himself",
  // "his",
  // "hither",
  // "hopefully",
  // "how",
  // "howbeit",
  // "however",
  // "i",
  // "i'd",
  // "i'll",
  // "i'm",
  // "i've",
  // "ie",
  // "if",
  // "ignored",
  // "immediate",
  // "in",
  // "inasmuch",
  // "inc",
  // "indeed",
  // "indicate",
  // "indicated",
  // "indicates",
  // "inner",
  // "insofar",
  // "instead",
  // "into",
  // "inward",
  // "is",
  // "isn't",
  // "it",
  // "it'd",
  // "it'll",
  // "it's",
  // "its",
  // "itself",
  // "j",
  // "just",
  // "k",
  // "keep",
  // "keeps",
  // "kept",
  // "know",
  // "knows",
  // "known",
  // "l",
  // "last",
  // "lately",
  // "later",
  // "latter",
  // "latterly",
  // "least",
  // "less",
  // "lest",
  // "let",
  // "let's",
  // "like",
  // "liked",
  // "likely",
  // "little",
  // "look",
  // "looking",
  // "looks",
  // "ltd",
  // "m",
  // "mainly",
  // "many",
  // "may",
  // "maybe",
  // "me",
  // "mean",
  // "meanwhile",
  // "merely",
  // "might",
  // "more",
  // "moreover",
  // "most",
  // "mostly",
  // "much",
  // "must",
  // "my",
  // "myself",
  // "n",
  // "name",
  // "namely",
  // "nd",
  // "near",
  // "nearly",
  // "necessary",
  // "need",
  // "needs",
  // "neither",
  // "never",
  // "nevertheless",
  // "new",
  // "next",
  // "nine",
  // "no",
  // "nobody",
  // "non",
  // "none",
  // "noone",
  // "nor",
  // "normally",
  // "not",
  // "nothing",
  // "novel",
  // "now",
  // "nowhere",
  // "o",
  // "obviously",
  // "of",
  // "off",
  // "often",
  // "oh",
  // "ok",
  // "okay",
  // "old",
  // "on",
  // "once",
  // "one",
  // "ones",
  // "only",
  // "onto",
  // "or",
  // "other",
  // "others",
  // "otherwise",
  // "ought",
  // "our",
  // "ours",
  // "ourselves",
  // "out",
  // "outside",
  // "over",
  // "overall",
  // "own",
  // "p",
  // "particular",
  // "particularly",
  // "per",
  // "perhaps",
  // "placed",
  // "please",
  // "plus",
  // "possible",
  // "presumably",
  // "probably",
  // "provides",
  // "q",
  // "que",
  // "quite",
  // "qv",
  // "r",
  // "rather",
  // "rd",
  // "re",
  // "really",
  // "reasonably",
  // "regarding",
  // "regardless",
  // "regards",
  // "relatively",
  // "respectively",
  // "right",
  // "s",
  // "said",
  // "same",
  // "saw",
  // "say",
  // "saying",
  // "says",
  // "second",
  // "secondly",
  // "see",
  // "seeing",
  // "seem",
  // "seemed",
  // "seeming",
  // "seems",
  // "seen",
  // "self",
  // "selves",
  // "sensible",
  // "sent",
  // "serious",
  // "seriously",
  // "seven",
  // "several",
  // "shall",
  // "she",
  // "should",
  // "shouldn't",
  // "since",
  // "six",
  // "so",
  // "some",
  // "somebody",
  // "somehow",
  // "someone",
  // "something",
  // "sometime",
  // "sometimes",
  // "somewhat",
  // "somewhere",
  // "soon",
  // "sorry",
  // "specified",
  // "specify",
  // "specifying",
  // "still",
  // "sub",
  // "such",
  // "sup",
  // "sure",
  // "t",
  // "t's",
  // "take",
  // "taken",
  // "tell",
  // "tends",
  // "th",
  // "than",
  // "thank",
  // "thanks",
  // "thanx",
  // "that",
  // "that's",
  // "thats",
  // "the",
  // "their",
  // "theirs",
  // "them",
  // "themselves",
  // "then",
  // "thence",
  // "there",
  // "there's",
  // "thereafter",
  // "thereby",
  // "therefore",
  // "therein",
  // "theres",
  // "thereupon",
  // "these",
  // "they",
  // "they'd",
  // "they'll",
  // "they're",
  // "they've",
  // "think",
  // "third",
  // "this",
  // "thorough",
  // "thoroughly",
  // "those",
  // "though",
  // "three",
  // "through",
  // "throughout",
  // "thru",
  // "thus",
  // "to",
  // "together",
  // "too",
  // "took",
  // "toward",
  // "towards",
  // "tried",
  // "tries",
  // "truly",
  // "try",
  // "trying",
  // "twice",
  // "two",
  // "u",
  // "un",
  // "under",
  // "unfortunately",
  // "unless",
  // "unlikely",
  // "until",
  // "unto",
  // "up",
  // "upon",
  // "us",
  // "use",
  // "used",
  // "useful",
  // "uses",
  // "using",
  // "usually",
  // "uucp",
  // "v",
  // "value",
  // "various",
  // "very",
  // "via",
  // "viz",
  // "vs",
  // "w",
  // "want",
  // "wants",
  // "was",
  // "wasn't",
  // "way",
  // "we",
  // "we'd",
  // "we'll",
  // "we're",
  // "we've",
  // "welcome",
  // "well",
  // "went",
  // "were",
  // "weren't",
  // "what",
  // "what's",
  // "whatever",
  // "when",
  // "whence",
  // "whenever",
  // "where",
  // "where's",
  // "whereafter",
  // "whereas",
  // "whereby",
  // "wherein",
  // "whereupon",
  // "wherever",
  // "whether",
  // "which",
  // "while",
  // "whither",
  // "who",
  // "who's",
  // "whoever",
  // "whole",
  // "whom",
  // "whose",
  // "why",
  // "will",
  // "willing",
  // "wish",
  // "with",
  // "within",
  // "without",
  // "won't",
  // "wonder",
  // "would",
  // "would",
  // "wouldn't",
  // "x",
  // "y",
  // "yes",
  // "yet",
  // "you",
  // "you'd",
  // "you'll",
  // "you're",
  // "you've",
  // "your",
  // "yours",
  // "yourself",
  // "yourselves",
  // "z",
  // "zero",
  // "http",
  // "www",
  // "com"
  // );
  
  // "presumably",
  // "probably",
  // "provides",
  // "q",
  // "que",
  // "quite",
  // "qv",
  // "r",
  // "rather",
  // "rd",
  // "re",
  // "really",
  // "reasonably",
  // "regarding",
  // "regardless",
  // "regards",
  // "relatively",
  // "respectively",
  // "right",
  // "s",
  // "said",
  // "same",
  // "saw",
  // "say",
  // "saying",
  // "says",
  // "second",
  // "secondly",
  // "see",
  // "seeing",
  // "seem",
  // "seemed",
  // "seeming",
  // "seems",
  // "seen",
  // "self",
  // "selves",
  // "sensible",
  // "sent",
  // "serious",
  // "seriously",
  // "seven",
  // "several",
  // "shall",
  // "she",
  // "should",
  // "shouldn't",
  // "since",
  // "six",
  // "so",
  // "some",
  // "somebody",
  // "somehow",
  // "someone",
  // "something",
  // "sometime",
  // "sometimes",
  // "somewhat",
  // "somewhere",
  // "soon",
  // "sorry",
  // "specified",
  // "specify",
  // "specifying",
  // "still",
  // "sub",
  // "such",
  // "sup",
  // "sure",
  // "t",
  // "t's",
  // "take",
  // "taken",
  // "tell",
  // "tends",
  // "th",
  // "than",
  // "thank",
  // "thanks",
  // "thanx",
  // "that",
  // "that's",
  // "thats",
  // "the",
  // "their",
  // "theirs",
  // "them",
  // "themselves",
  // "then",
  // "thence",
  // "there",
  // "there's",
  // "thereafter",
  // "thereby",
  // "therefore",
  // "therein",
  // "theres",
  // "thereupon",
  // "these",
  // "they",
  // "they'd",
  // "they'll",
  // "they're",
  // "they've",
  // "think",
  // "third",
  // "this",
  // "thorough",
  // "thoroughly",
  // "those",
  // "though",
  // "three",
  // "through",
  // "throughout",
  // "thru",
  // "thus",
  // "to",
  // "together",
  // "too",
  // "took",
  // "toward",
  // "towards",
  // "tried",
  // "tries",
  // "truly",
  // "try",
  // "trying",
  // "twice",
  // "two",
  // "u",
  // "un",
  // "under",
  // "unfortunately",
  // "unless",
  // "unlikely",
  // "until",
  // "unto",
  // "up",
  // "upon",
  // "us",
  // "use",
  // "used",
  // "useful",
  // "uses",
  // "using",
  // "usually",
  // "uucp",
  // "v",
  // "value",
  // "various",
  // "very",
  // "via",
  // "viz",
  // "vs",
  // "w",
  // "want",
  // "wants",
  // "was",
  // "wasn't",
  // "way",
  // "we",
  // "we'd",
  // "we'll",
  // "we're",
  // "we've",
  // "welcome",
  // "well",
  // "went",
  // "were",
  // "weren't",
  // "what",
  // "what's",
  // "whatever",
  // "when",
  // "whence",
  // "whenever",
  // "where",
  // "where's",
  // "whereafter",
  // "whereas",
  // "whereby",
  // "wherein",
  // "whereupon",
  // "wherever",
  // "whether",
  // "which",
  // "while",
  // "whither",
  // "who",
  // "who's",
  // "whoever",
  // "whole",
  // "whom",
  // "whose",
  // "why",
  // "will",
  // "willing",
  // "wish",
  // "with",
  // "within",
  // "without",
  // "won't",
  // "wonder",
  // "would",
  // "would",
  // "wouldn't",
  // "x",
  // "y",
  // "yes",
  // "yet",
  // "you",
  // "you'd",
  // "you'll",
  // "you're",
  // "you've",
  // "your",
  // "yours",
  // "yourself",
  // "yourselves",
  // "z",
  // "zero",
  // "http",
  // "www",
  // "com"
  // );
  
}