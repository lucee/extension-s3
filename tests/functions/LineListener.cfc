component  {

	variables.lineListener="";


	function before() {
		variables.lineListener="before;";
	}
	function invoke(line) {
		variables.lineListener&=line&";";
		return true;
	}
	function after() {
		variables.lineListener&="after;";
	}

	function getData() {
		return variables.lineListener;
	}
}