component  {

	variables.listener="";


	function before() {
		variables.listener="before;";
	}
	function invoke(binary4) {
		variables.listener&=len(binary4)&";";
		return true;
	}
	function after() {
		variables.listener&="after;";
	}

	function getData() {
		return variables.listener;
	}
}