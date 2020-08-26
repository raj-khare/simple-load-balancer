# to quickly get this project running

FROM golang:latest AS builder
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 GOOS=linux go build -o lb .

FROM alpine:latest  
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /src/lb .


# if you have golang locally installed then compile it using "RUN CGO_ENABLED=0 GOOS=linux go build -o lb ." and uncomment the code above

#FROM alpine:latest  
#RUN apk --no-cache add ca-certificates
#WORKDIR /root/
#COPY lb .
