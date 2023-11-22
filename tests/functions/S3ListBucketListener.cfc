component  {
	variables.res="";

    function before(){
        variables.res&="-->";
    }
    function after(){
        variables.res&="<--";
    }

    function invoke(data){
        variables.res&=data.recordcount&";";
    }

    function getData() {
        return  variables.res;
    }
}