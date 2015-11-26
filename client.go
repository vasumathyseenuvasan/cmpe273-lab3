package main

import (
  "fmt"
  "net/http"
  m "encoding/json"
  "io/ioutil"
  "github.com/julienschmidt/httprouter"
  "bytes"
  "strconv"
  "hash/fnv"
  "sort"
  "log"
)

type Data struct{
  Key int `json:"key"`
  Value string `json:"value"`
}

type Response struct{
  DataResponse []Data `json:"Response"`
}

type SortedCircle []Data
var data map[int]string
var nodes []string
var numberofReplicas int
var sortedCircle SortedCircle

func addNodesOnCircle(node string){
    for i:=0; i<numberofReplicas; i++{
      var data Data
      hashValue:=fnv.New32a()
      hashValue.Write([]byte(node))
      hash:=int(hashValue.Sum32()+uint32(i))
      hash=hash+320000000
      data.Key=hash
      data.Value=node
      sortedCircle=append(sortedCircle,data)
    }
}

func (slice SortedCircle) Len() int {
    return len(slice)
}
func (slice SortedCircle) Less(i, j int) bool {
    return slice[i].Key < slice[j].Key;
}
func (slice SortedCircle) Swap(i, j int) {
    slice[i], slice[j] = slice[j], slice[i]
}

func getNode(key int)string{
  nodeNumber :=""
  hash:=fnv.New32a()
  hash.Write([]byte(strconv.Itoa(key)))
  hashValue:=int(hash.Sum32())
  for _,data:=range sortedCircle{
    if(data.Key>=hashValue){
        nodeNumber=data.Value
        break
      }
  }
  if(nodeNumber==""){
    data:=sortedCircle[0]
    nodeNumber=data.Value
  }
  return nodeNumber
}

func main() {
        nodes = []string{"3000","3001","3002"}
        numberofReplicas = 100
        for _,node:=range nodes{
          addNodesOnCircle(node)
        }
        sort.Sort(sortedCircle)
        data = map[int]string{1:"a",2:"b",3:"c",4:"d",5:"e",
                          6:"f",7:"g",8:"h",9:"i",10:"j"}
        for key,value:=range data{
          var objData Data
          objData.Key = key
          objData.Value = value
          JsonData,_:=m.Marshal(objData)
          nodeNumber:=getNode(key)
          fmt.Print("Node Number: ")
          fmt.Println(nodeNumber)
          fmt.Print("key value: ")
          fmt.Println(key)
          url:= "http://localhost:"+nodeNumber+"/keys"
          http.Post(url, "application/json", bytes.NewBuffer(JsonData))
        }
        mux := httprouter.New()
        mux.GET("/keys/:key_id", findKeys)
        mux.GET("/keys", findAllKeys)
        mux.PUT("/keys/:key_id/:value",updateKey)
        server := http.Server{
                Addr:        ":8080",
                Handler: mux,
        }
        server.ListenAndServe()
}

func updateKey(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  keyId,_:=strconv.Atoi(p.ByName("key_id"))
  keyValue:=p.ByName("value")
  nodeNumber:=getNode(keyId)
  url:="http://localhost:"+nodeNumber+"/keys/"+p.ByName("key_id")+"/"+keyValue
  req, err := http.NewRequest("PUT", url, nil)
  client := &http.Client{}
  resp, errors := client.Do(req)
    if(errors!=nil){
      panic(err)
    }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  var keyValueResp Data
  m.Unmarshal(body, &keyValueResp)
  rw.WriteHeader(http.StatusOK)
  rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
  if err := m.NewEncoder(rw).Encode(keyValueResp); err != nil {
     panic(err)
 }
}

func findKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  keyId,_:=strconv.Atoi(p.ByName("key_id"))
  nodeNumber:=getNode(keyId)
  url:="http://localhost:"+nodeNumber+"/keys/"+p.ByName("key_id")
  resp,err:= http.Get(url)
  if err != nil {
		log.Fatal()
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
  var keyValue Data
  m.Unmarshal(body, &keyValue)
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

func findAllKeys(rw http.ResponseWriter, req *http.Request, p httprouter.Params){
  var finalResponse Response
  for _,node:=range nodes{
    keyValueResponse:=getKeyValues(node)
    for _,data:=range keyValueResponse.DataResponse{
      finalResponse.DataResponse=append(finalResponse.DataResponse,data)
    }
  }
  rw.WriteHeader(http.StatusOK)
  rw.Header().Set("Content-Type", "application/json;charset=UTF-8")
  if err := m.NewEncoder(rw).Encode(finalResponse); err != nil {
     panic(err)
 }
}

func getKeyValues(nodeNumber string) Response{
  var unmarshalResponse Response
  url:="http://localhost:"+nodeNumber+"/keys"
  resp,err:= http.Get(url)
  if err != nil {
    log.Fatal()
  }
  defer resp.Body.Close()
  body, err := ioutil.ReadAll(resp.Body)
  m.Unmarshal(body, &unmarshalResponse)
  return unmarshalResponse
}
