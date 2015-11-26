package main

import(
  "io"
  "fmt"
  m "encoding/json"
  "io/ioutil"
  "net/http"
  "github.com/julienschmidt/httprouter"
  "strconv"
)

type Data struct{
  Key int `json:"key"`
  Value string `json:"value"`
}

type Response struct{
  DataResponse []Data `json:"Response"`
}

var dataCollection []Data

func addKeys(rw http.ResponseWriter, req *http.Request , p httprouter.Params) {
  fmt.Println("entering service")
    var objData Data
    body,_ := ioutil.ReadAll(io.LimitReader(req.Body, 1048576))
    m.Unmarshal(body, &objData)
    dataCollection=append(dataCollection,objData)
    rw.WriteHeader(http.StatusCreated)
    rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
    rw.WriteHeader(http.StatusCreated)
}

func findKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  keyId,_:=strconv.Atoi(p.ByName("key_id"))
  var keyValue Data
  for _,value:=range dataCollection{
    if(value.Key==keyId){
      keyValue=value;
      break;
    }
  }
  if(keyValue.Key==0){
      rw.WriteHeader(http.StatusOK)
      fmt.Fprintf(rw,"Requested keyId not found")
      return
  }
  rw.WriteHeader(http.StatusOK)
  rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
  if err := m.NewEncoder(rw).Encode(keyValue); err != nil {
     panic(err)
 }
}

func updateKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  keyId,_:=strconv.Atoi(p.ByName("key_id"))
  keyValue:=p.ByName("value")
  var dataResp Data
  for i,_:=range dataCollection{
    objData:=&dataCollection[i]
    if(objData.Key==keyId){
      objData.Value=keyValue;
      dataResp.Value=keyValue;
      dataResp.Key=keyId;
      break;
    }
  }
  rw.WriteHeader(http.StatusOK)
  rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
  if err := m.NewEncoder(rw).Encode(dataResp); err != nil {
     panic(err)
 }
}

func findAllKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  var response Response
  response.DataResponse =  dataCollection
  rw.WriteHeader(http.StatusOK)
  rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
  if err := m.NewEncoder(rw).Encode(response); err != nil {
     panic(err)
 }
}

func main(){
mux := httprouter.New()
mux.GET("/keys/:key_id", findKeys)
mux.POST("/keys", addKeys)
mux.GET("/keys", findAllKeys)
mux.PUT("/keys/:key_id/:value",updateKey)
server := http.Server{
        Addr:        ":3002",
        Handler: mux,
}
server.ListenAndServe()
}
